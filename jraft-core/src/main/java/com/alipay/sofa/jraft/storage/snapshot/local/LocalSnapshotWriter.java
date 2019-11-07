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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta.Builder;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Snapshot writer to write snapshot into local file system.
 * 基于本地文件系统的 快照写入对象  实际上写入由用户实现 最后通过addFile 向快照文件信息注册到这里
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 11:51:43 AM
 */
public class LocalSnapshotWriter extends SnapshotWriter {

    private static final Logger          LOG = LoggerFactory.getLogger(LocalSnapshotWriter.class);

    /**
     * 维护 文件名与元数据的映射关系 用于快速定位  read内部应该也有该属性
     */
    private final LocalSnapshotMetaTable metaTable;
    private final String                 path;
    /**
     * 本地快照存储器  看来外部是通过该对象向 storage中写入数据的
     */
    private final LocalSnapshotStorage   snapshotStorage;

    /**
     * 通过 指定文件夹路径 和 storage 对象初始化
     * @param path
     * @param snapshotStorage
     * @param raftOptions
     */
    public LocalSnapshotWriter(String path, LocalSnapshotStorage snapshotStorage, RaftOptions raftOptions) {
        super();
        this.snapshotStorage = snapshotStorage;
        // 写入的目标文件路径  实际上对应到一个 temp文件  在调用storage.close 时会将temp文件转换为持久文件(携带快照下标的文件)
        this.path = path;
        this.metaTable = new LocalSnapshotMetaTable(raftOptions);
    }

    @Override
    public boolean init(final Void v) {
        // 根据临时文件路径创建文件夹
        final File dir = new File(this.path);
        try {
            FileUtils.forceMkdir(dir);
        } catch (final IOException e) {
            LOG.error("Fail to create directory {}.", this.path);
            setError(RaftError.EIO, "Fail to create directory  %s", this.path);
            return false;
        }
        // 除了快照文件本身还会创建一个 元数据相关的文件
        final String metaPath = path + File.separator + JRAFT_SNAPSHOT_META_FILE;
        final File metaFile = new File(metaPath);
        try {
            // 如果已经存在文件夹 就加载旧的数据
            if (metaFile.exists()) {
                return metaTable.loadFromFile(metaPath);
            }
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot meta from {}.", metaPath);
            setError(RaftError.EIO, "Fail to load snapshot meta from %s", metaPath);
            return false;
        }
        return true;
    }

    /**
     * 获取快照数据 对应的最后写入的index  这个index 应该就是对应LogEntry 的 index
     * @return
     */
    public long getSnapshotIndex() {
        return this.metaTable.hasMeta() ? this.metaTable.getMeta().getLastIncludedIndex() : 0;
    }

    @Override
    public void shutdown() {
        Utils.closeQuietly(this);
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    /**
     * 关闭同时会将 temp 文件持久化
     * @param keepDataOnError whether to keep data when error happens. 当出现异常时 是否停止删除
     * @throws IOException
     */
    @Override
    public void close(final boolean keepDataOnError) throws IOException {
        this.snapshotStorage.close(this, keepDataOnError);
    }

    /**
     * 将元数据 保存安东 metaTable 中
     * @param meta snapshot metadata
     * @return
     */
    @Override
    public boolean saveMeta(final SnapshotMeta meta) {
        this.metaTable.setMeta(meta);
        return true;
    }

    /**
     * 等待元数据写入到文件中
     * @return
     * @throws IOException
     */
    public boolean sync() throws IOException {
        return this.metaTable.saveToFile(this.path + File.separator + JRAFT_SNAPSHOT_META_FILE);
    }

    /**
     * 当使用该写入对象成功写入文件后 将文件交由该对象管理
     * @param fileName file name
     * @param fileMeta file metadata
     * @return
     */
    @Override
    public boolean addFile(final String fileName, final Message fileMeta) {
        final Builder metaBuilder = LocalFileMeta.newBuilder();
        if (fileMeta != null) {
            metaBuilder.mergeFrom(fileMeta);
        }
        final LocalFileMeta meta = metaBuilder.build();
        return this.metaTable.addFile(fileName, meta);
    }

    @Override
    public boolean removeFile(final String fileName) {
        return this.metaTable.removeFile(fileName);
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
