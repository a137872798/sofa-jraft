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
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.FileService;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Snapshot reader on local file system.
 * 读取基于文件系统的 快照对象
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 11:10:34 AM
 */
public class LocalSnapshotReader extends SnapshotReader {

    private static final Logger          LOG = LoggerFactory.getLogger(LocalSnapshotReader.class);

    /** Generated reader id 当初始化时 该值为0 */
    private long                         readerId;
    /** remote peer addr */
    private final Endpoint               addr;
    /**
     * 存放本地快照元数据的 table
     */
    private final LocalSnapshotMetaTable metaTable;
    private final String                 path;
    private final LocalSnapshotStorage   snapshotStorage;
    /**
     * 一个限制对象
     */
    private final SnapshotThrottle       snapshotThrottle;

    /**
     * 关闭对象就是减少引用计数
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        snapshotStorage.unref(this.getSnapshotIndex());
        // 关闭reader
        this.destroyReaderInFileService();
    }

    /**
     * 初始化reader 对象
     * @param snapshotStorage
     * @param snapshotThrottle
     * @param addr
     * @param raftOptions
     * @param path
     */
    public LocalSnapshotReader(LocalSnapshotStorage snapshotStorage, SnapshotThrottle snapshotThrottle, Endpoint addr,
                               RaftOptions raftOptions, String path) {
        super();
        this.snapshotStorage = snapshotStorage;
        this.snapshotThrottle = snapshotThrottle;
        this.addr = addr;
        this.path = path;
        this.readerId = 0;
        // 生成快照元数据
        this.metaTable = new LocalSnapshotMetaTable(raftOptions);
    }

    @OnlyForTest
    long getReaderId() {
        return this.readerId;
    }

    /**
     * 初始化reader 对象
     * @param v
     * @return
     */
    @Override
    public boolean init(final Void v) {
        // 这里的 path 就是 storage 的 快照文件路径
        final File dir = new File(this.path);
        if (!dir.exists()) {
            LOG.error("No such path %s for snapshot reader.", this.path);
            setError(RaftError.ENOENT, "No such path %s for snapshot reader", this.path);
            return false;
        }
        // 拼接成一个新的文件路径
        final String metaPath = this.path + File.separator + JRAFT_SNAPSHOT_META_FILE;
        try {
            // 初始化时 storage 对应的文件中读取数据 并存入 metaTable 中
            return this.metaTable.loadFromFile(metaPath);
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot meta {}.", metaPath);
            setError(RaftError.EIO, "Fail to load snapshot meta from path %s", metaPath);
            return false;
        }
    }

    /**
     * 获取快照下标
     * @return
     */
    private long getSnapshotIndex() {
        final File file = new File(this.path);
        final String name = file.getName();
        // 确保storage用于保存快照的文件必须以 snapshot 开头
        if (!name.startsWith(JRAFT_SNAPSHOT_PREFIX)) {
            throw new IllegalStateException("Invalid snapshot path name:" + name);
        }
        // 后面的内容代表 快照的偏移量  看来每次的快照数据都会生成一个独有的文件
        return Long.parseLong(name.substring(JRAFT_SNAPSHOT_PREFIX.length()));
    }

    @Override
    public void shutdown() {
        Utils.closeQuietly(this);
    }

    /**
     * 加载 快照元数据
     * @return
     */
    @Override
    public SnapshotMeta load() {
        if (this.metaTable.hasMeta()) {
            return this.metaTable.getMeta();
        }
        return null;
    }

    /**
     * 生成一个 URL
     * @return
     */
    @Override
    public String generateURIForCopy() {
        if (this.addr == null || this.addr.equals(new Endpoint(Utils.IP_ANY, 0))) {
            LOG.error("Address is not specified");
            return null;
        }
        // 为什么一定要readerId 为0 ???
        if (this.readerId == 0) {
            // 创建一个快照文件读取对象
            final SnapshotFileReader reader = new SnapshotFileReader(this.path, this.snapshotThrottle);
            reader.setMetaTable(this.metaTable);
            // 读取失败
            if (!reader.open()) {
                LOG.error("Open snapshot {} failed.", this.path);
                return null;
            }
            // 添加reader 对象
            this.readerId = FileService.getInstance().addReader(reader);
            if (this.readerId < 0) {
                LOG.error("Fail to add reader to file_service.");
                return null;
            }
        }

        return String.format(REMOTE_SNAPSHOT_URI_SCHEME + "%s/%d", this.addr.toString(), this.readerId);
    }

    /**
     * 关闭 reader 对象
     */
    private void destroyReaderInFileService() {
        if (this.readerId > 0) {
            // 如果readerId 有效  从fileService 中移除 reader
            FileService.getInstance().removeReader(this.readerId);
            this.readerId = 0;
        } else {
            if (this.readerId != 0) {
                LOG.warn("Ignore destroy invalid readerId: {}", this.readerId);
            }
        }
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public Set<String> listFiles() {
        return this.metaTable.listFiles();
    }

    @Override
    public Message getFileMeta(final String fileName) {
        return this.metaTable.getFileMeta(fileName);
    }
}
