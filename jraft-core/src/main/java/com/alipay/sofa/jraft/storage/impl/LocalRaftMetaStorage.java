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
package com.alipay.sofa.jraft.storage.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.LocalStorageOutter.StablePBMeta;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.io.ProtoBufFile;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Raft meta storage,it's not thread-safe.
 * 记录本节点当前所在任期 以及投票给了哪个节点
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-26 7:30:36 PM
 */
public class LocalRaftMetaStorage implements RaftMetaStorage {

    private static final Logger LOG       = LoggerFactory.getLogger(LocalRaftMetaStorage.class);
    private static final String RAFT_META = "raft_meta";

    /**
     * 是否初始化完毕
     */
    private boolean             isInited;
    /**
     * 存储路径  看来是基于本地文件存储
     */
    private final String        path;
    /**
     * 当前任期
     */
    private long                term;
    /** blank votedFor information 默认情况下没有投票目标*/
    private PeerId              votedFor  = PeerId.emptyPeer();
    private final RaftOptions   raftOptions;
    private NodeMetrics         nodeMetrics;
    /**
     * node
     */
    private NodeImpl            node;

    public LocalRaftMetaStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        this.raftOptions = raftOptions;
    }

    /**
     * 初始化  jraft的套路是通过一个options 进行初始化 该对象内部维护了init 需要的其他属性
     * @param opts
     * @return
     */
    @Override
    public boolean init(final RaftMetaStorageOptions opts) {
        if (this.isInited) {
            LOG.warn("Raft meta storage is already inited.");
            return true;
        }
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        try {
            // 创建文件
            FileUtils.forceMkdir(new File(this.path));
        } catch (final IOException e) {
            LOG.error("Fail to mkdir {}", this.path);
            return false;
        }
        // 针对文件已经存在的情况下 将数据加载到内存来
        if (load()) {
            this.isInited = true;
            return true;
        } else {
            return false;
        }
    }

    /**
     * 从文件中加载数据
     * @return
     */
    private boolean load() {
        final ProtoBufFile pbFile = newPbFile();
        try {
            // 尝试加载 this.path + File.separator + RAFT_META 路径下文件的数据  持久化时 将任期和 投票对象 序列化成StablePBMeta 并存入到 文件中
            final StablePBMeta meta = pbFile.load();
            if (meta != null) {
                this.term = meta.getTerm();
                return this.votedFor.parse(meta.getVotedfor());
            }
            return true;
        } catch (final FileNotFoundException e) {
            // 可能特殊文件还未生成
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load raft meta storage", e);
            return false;
        }
    }

    private ProtoBufFile newPbFile() {
        // 该对象内部封装了按照特殊格式读取文件的逻辑
        return new ProtoBufFile(this.path + File.separator + RAFT_META);
    }

    /**
     * 存储数据
     * @return
     */
    private boolean save() {
        final long start = Utils.monotonicMs();
        final StablePBMeta meta = StablePBMeta.newBuilder() //
            .setTerm(this.term) //
            .setVotedfor(this.votedFor.toString()) //
            .build();
        final ProtoBufFile pbFile = newPbFile();
        try {
            // 将数据保存到 特殊文件中
            if (!pbFile.save(meta, this.raftOptions.isSyncMeta())) {
                reportIOError();
                return false;
            }
            return true;
        } catch (final Exception e) {
            LOG.error("Fail to save raft meta", e);
            reportIOError();
            return false;
        } finally {
            final long cost = Utils.monotonicMs() - start;
            if (this.nodeMetrics != null) {
                this.nodeMetrics.recordLatency("save-raft-meta", cost);
            }
            LOG.info("Save raft meta, path={}, term={}, votedFor={}, cost time={} ms", this.path, this.term,
                this.votedFor, cost);
        }
    }

    private void reportIOError() {
        this.node.onError(new RaftException(ErrorType.ERROR_TYPE_META, RaftError.EIO,
            "Fail to save raft meta, path=%s", this.path));
    }

    @Override
    public void shutdown() {
        if (!this.isInited) {
            return;
        }
        // 关闭时先将当前信息 格式化后存入文件 对应到 init() 方法中的load()
        save();
        this.isInited = false;
    }

    private void checkState() {
        if (!this.isInited) {
            throw new IllegalStateException("LocalRaftMetaStorage not initialized");
        }
    }

    @Override
    public boolean setTerm(final long term) {
        checkState();
        this.term = term;
        return save();
    }

    @Override
    public long getTerm() {
        checkState();
        return this.term;
    }

    @Override
    public boolean setVotedFor(final PeerId peerId) {
        checkState();
        this.votedFor = peerId;
        return save();
    }

    @Override
    public PeerId getVotedFor() {
        checkState();
        return this.votedFor;
    }

    /**
     * 修改数据并写入文件(持久化)
     * @param term
     * @param peerId
     * @return
     */
    @Override
    public boolean setTermAndVotedFor(final long term, final PeerId peerId) {
        checkState();
        this.votedFor = peerId;
        this.term = term;
        return save();
    }

    @Override
    public String toString() {
        return "RaftMetaStorageImpl [path=" + this.path + ", term=" + this.term + ", votedFor=" + this.votedFor + "]";
    }
}
