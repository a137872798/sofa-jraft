/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.storage.snapshot.remote;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.option.CopyOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Remote file copier
 * 远程文件 copier
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-23 2:03:14 PM
 */
public class RemoteFileCopier {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteFileCopier.class);

    private long                readId;
    /**
     * 客户端服务
     */
    private RaftClientService   rpcService;
    /**
     * 定位到 远端地址
     */
    private Endpoint            endpoint;
    private RaftOptions         raftOptions;
    /**
     * 定时器管理对象
     */
    private TimerManager        timerManager;
    /**
     * 阀门对象
     */
    private SnapshotThrottle    snapshotThrottle;

    @OnlyForTest
    long getReaderId() {
        return this.readId;
    }

    @OnlyForTest
    Endpoint getEndpoint() {
        return this.endpoint;
    }

    /**
     * 初始化
     * @param uri  对应被拷贝的 远端地址
     * @param snapshotThrottle  过滤数据的 阀门对象
     * @param opts
     * @return
     */
    public boolean init(String uri, final SnapshotThrottle snapshotThrottle, final SnapshotCopierOptions opts) {
        // node 对象
        this.rpcService = opts.getRaftClientService();
        this.timerManager = opts.getTimerManager();
        this.raftOptions = opts.getRaftOptions();
        // 阀门
        this.snapshotThrottle = snapshotThrottle;

        // 远程拉取url 前缀
        final int prefixSize = Snapshot.REMOTE_SNAPSHOT_URI_SCHEME.length();
        if (uri == null || !uri.startsWith(Snapshot.REMOTE_SNAPSHOT_URI_SCHEME)) {
            LOG.error("Invalid uri {}.", uri);
            return false;
        }
        uri = uri.substring(prefixSize);
        final int slasPos = uri.indexOf('/');
        // 截取 ip, port
        final String ipAndPort = uri.substring(0, slasPos);
        uri = uri.substring(slasPos + 1);

        try {
            // 解析出对应的 readId  在 leader 每次发起安装快照请求时就会创建一个 snapshotReader 对象 会对应一个readId
            this.readId = Long.parseLong(uri);
            final String[] ipAndPortStrs = ipAndPort.split(":");
            // 将远端 ip port 转换成 endpoint 对象 实际上就是leader 的地址
            this.endpoint = new Endpoint(ipAndPortStrs[0], Integer.parseInt(ipAndPortStrs[1]));
        } catch (final Exception e) {
            LOG.error("Fail to parse readerId or endpoint.", e);
            return false;
        }
        // 连接到目标服务器
        if (!this.rpcService.connect(this.endpoint)) {
            LOG.error("Fail to init channel to {}.", this.endpoint);
            return false;
        }

        return true;
    }

    /**
     * Copy `source` from remote to local dest.
     * 将数据拷贝到文件中
     * @param source   source from remote  相当于定位远端字段的 标识符
     * @param destPath local path
     * @param opts     options of copy
     * @return true if copy success
     */
    public boolean copyToFile(final String source, final String destPath, final CopyOptions opts) throws IOException,
                                                                                                 InterruptedException {
        // 根据 source 定位到资源 并将数据保存到 目标路径下 同时返回一个会话对象
        final Session session = startCopyToFile(source, destPath, opts);
        if (session == null) {
            return false;
        }
        try {
            // 阻塞直到写入完成 这个session 类似于future
            session.join();
            return session.status().isOk();
        } finally {
            Utils.closeQuietly(session);
        }
    }

    /**
     * 从 remote 指定路径下将数据拷贝到 目标路径
     * @param source
     * @param destPath
     * @param opts
     * @return
     * @throws IOException
     */
    public Session startCopyToFile(final String source, final String destPath, final CopyOptions opts)
                                                                                                      throws IOException {
        final File file = new File(destPath);

        // delete exists file.
        if (file.exists()) {
            if (!file.delete()) {
                LOG.error("Fail to delete destPath: {}.", destPath);
                return null;
            }
        }

        // 这里通过file 定位了outputStream 的输出路径
        final OutputStream out = new BufferedOutputStream(new FileOutputStream(file, false) {

            // 通过 重写close 逻辑实现 物理级别同步写入
            @Override
            public void close() throws IOException {
                getFD().sync();
                super.close();
            }
        });
        // 目标快照文件名作为 source  构建会话对象
        final BoltSession session = newBoltSession(source);
        // 当输出到文件时 不设置 buf 而是设置 outputStream
        session.setOutputStream(out);
        session.setDestPath(destPath);
        session.setDestBuf(null);
        // 将opts 设置到 session 中
        if (opts != null) {
            session.setCopyOptions(opts);
        }
        // 开始通过会话对象拉取数据 并将结果设置到 outputstream 对应的文件中
        session.sendNextRpc();
        return session;
    }

    /**
     * 构建会话对象
     * @param source
     * @return
     */
    private BoltSession newBoltSession(final String source) {
        // 生成一个拉取文件的请求 并指定了 readId 这样会在 leader找到对应的reader
        final GetFileRequest.Builder reqBuilder = GetFileRequest.newBuilder() //
            .setFilename(source) //
            .setReaderId(this.readId);
        // 通过leader 阀门 等 构建一个拉取快照数据的会话对象
        return new BoltSession(this.rpcService, this.timerManager, this.snapshotThrottle, this.raftOptions, reqBuilder,
            this.endpoint);
    }

    /**
     * Copy `source` from remote to  buffer.
     * 将数据拷贝到 IOBuffer 中
     * @param source  source from remote
     * @param destBuf buffer of dest
     * @param opt     options of copy
     * @return true if copy success
     */
    public boolean copy2IoBuffer(final String source, final ByteBufferCollector destBuf, final CopyOptions opt)
                                                                                                               throws InterruptedException {
        final Session session = startCopy2IoBuffer(source, destBuf, opt);
        if (session == null) {
            return false;
        }
        try {
            session.join();
            return session.status().isOk();
        } finally {
            Utils.closeQuietly(session);
        }
    }

    /**
     * 将数据保存到IOBuffer 中
     * @param source  "__raft_snapshot_meta"
     * @param destBuf   用于存储元数据的buffer
     * @param opts
     * @return
     */
    public Session startCopy2IoBuffer(final String source, final ByteBufferCollector destBuf, final CopyOptions opts) {
        // 通过给定的字符串生成一个会话对象
        final BoltSession session = newBoltSession(source);
        // destion 为 buffer 时 不设置outputStream 设置destBuf
        session.setOutputStream(null);
        session.setDestBuf(destBuf);
        if (opts != null) {
            session.setCopyOptions(opts);
        }
        // 通过session 对象拉取数据
        session.sendNextRpc();
        return session;
    }
}
