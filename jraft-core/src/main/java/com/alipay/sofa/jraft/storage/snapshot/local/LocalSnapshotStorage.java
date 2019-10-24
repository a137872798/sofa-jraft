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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Snapshot storage based on local file storage.
 * 快照存储 同样是基于本地文件
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 2:11:30 PM
 */
public class LocalSnapshotStorage implements SnapshotStorage {

    private static final Logger                      LOG       = LoggerFactory.getLogger(LocalSnapshotStorage.class);

    private static final String                      TEMP_PATH = "temp";
    /**
     * key 代表快照文件的 index  value 代表对该index 的引用计数
     */
    private final ConcurrentMap<Long, AtomicInteger> refMap    = new ConcurrentHashMap<>();
    /**
     * 存储路径
     */
    private final String                             path;
    /**
     * 节点的地址 被抽象成一个 Endpoint 对象
     */
    private Endpoint                                 addr;
    private boolean                                  filterBeforeCopyRemote;
    /**
     * 代表快照文件的 下标 而不是某个 偏移量
     */
    private long                                     lastSnapshotIndex;
    private final Lock                               lock;
    private final RaftOptions                        raftOptions;
    /**
     * 快照阀门???  推测是配合 filterBeforeCopyRemote 起作用
     */
    private SnapshotThrottle                         snapshotThrottle;

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public boolean hasServerAddr() {
        return this.addr != null;
    }

    public void setServerAddr(Endpoint addr) {
        this.addr = addr;
    }

    public LocalSnapshotStorage(String path, RaftOptions raftOptions) {
        super();
        this.path = path;
        this.lastSnapshotIndex = 0;
        this.raftOptions = raftOptions;
        this.lock = new ReentrantLock();
    }

    public long getLastSnapshotIndex() {
        this.lock.lock();
        try {
            return this.lastSnapshotIndex;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 初始化
     * @param v
     * @return
     */
    @Override
    public boolean init(final Void v) {
        // 创建存储快照的文件夹
        final File dir = new File(this.path);

        try {
            FileUtils.forceMkdir(dir);
        } catch (final IOException e) {
            LOG.error("Fail to create directory {}.", this.path);
            return false;
        }

        // delete temp snapshot
        if (!this.filterBeforeCopyRemote) {
            final String tempSnapshotPath = this.path + File.separator + TEMP_PATH;
            final File tempFile = new File(tempSnapshotPath);
            if (tempFile.exists()) {
                try {
                    // 删除临时文件
                    FileUtils.forceDelete(tempFile);
                } catch (final IOException e) {
                    LOG.error("Fail to delete temp snapshot path {}.", tempSnapshotPath);
                    return false;
                }
            }
        }
        // delete old snapshot
        final List<Long> snapshots = new ArrayList<>();
        final File[] oldFiles = dir.listFiles();
        if (oldFiles != null) {
            for (final File sFile : oldFiles) {
                final String name = sFile.getName();
                if (!name.startsWith(Snapshot.JRAFT_SNAPSHOT_PREFIX)) {
                    continue;
                }
                // index 代表是第几个快照文件
                final long index = Long.parseLong(name.substring(Snapshot.JRAFT_SNAPSHOT_PREFIX.length()));
                snapshots.add(index);
            }
        }

        // TODO: add snapshot watcher

        // get last_snapshot_index   里面存放的是被删除的快照文件的 index 每个index 对应该文件是第一个快照
        if (!snapshots.isEmpty()) {
            Collections.sort(snapshots);
            final int snapshotCount = snapshots.size();

            for (int i = 0; i < snapshotCount - 1; i++) {
                final long index = snapshots.get(i);
                // 找到对应的路径(这里应该是文件夹路径)
                final String snapshotPath = getSnapshotPath(index);
                // 删除最后个之前的其余文件
                if (!destroySnapshot(snapshotPath)) {
                    return false;
                }
            }
            this.lastSnapshotIndex = snapshots.get(snapshotCount - 1);
            // 增加引用计数
            ref(this.lastSnapshotIndex);
        }

        return true;
    }

    /**
     * 看来 设置的path 只是文件名的前缀  在真正做存储的时候会加上 PREFIX + 第几个文件
     * @param index
     * @return
     */
    private String getSnapshotPath(final long index) {
        return this.path + File.separator + Snapshot.JRAFT_SNAPSHOT_PREFIX + index;
    }

    void ref(final long index) {
        final AtomicInteger refs = getRefs(index);
        refs.incrementAndGet();
    }

    /**
     * 删除对应文件
     * @param path
     * @return
     */
    private boolean destroySnapshot(final String path) {
        LOG.info("Deleting snapshot {}.", path);
        final File file = new File(path);
        try {
            FileUtils.deleteDirectory(file);
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to destroy snapshot {}.", path);
            return false;
        }
    }

    /**
     * 释放引用计数
     * @param index
     */
    void unref(final long index) {
        final AtomicInteger refs = getRefs(index);
        if (refs.decrementAndGet() == 0) {
            if (this.refMap.remove(index, refs)) {
                destroySnapshot(getSnapshotPath(index));
            }
        }
    }

    /**
     * 获取对应下标的 引用计数
     * @param index
     * @return
     */
    AtomicInteger getRefs(final long index) {
        AtomicInteger refs = this.refMap.get(index);
        if (refs == null) {
            refs = new AtomicInteger(0);
            final AtomicInteger eRefs = this.refMap.putIfAbsent(index, refs);
            if (eRefs != null) {
                refs = eRefs;
            }
        }
        return refs;
    }

    /**
     * 必须确保 引用数为0 才能关闭文件
     * @param writer
     * @param keepDataOnError
     * @throws IOException
     */
    void close(final LocalSnapshotWriter writer, final boolean keepDataOnError) throws IOException {
        int ret = writer.getCode();
        // noinspection ConstantConditions
        do {
            if (ret != 0) {
                break;
            }
            try {
                if (!writer.sync()) {
                    ret = RaftError.EIO.getNumber();
                    break;
                }
            } catch (final IOException e) {
                LOG.error("Fail to sync writer {}.", writer.getPath());
                ret = RaftError.EIO.getNumber();
                break;
            }
            final long oldIndex = getLastSnapshotIndex();
            final long newIndex = writer.getSnapshotIndex();
            if (oldIndex == newIndex) {
                ret = RaftError.EEXISTS.getNumber();
                break;
            }
            // rename temp to new
            final String tempPath = this.path + File.separator + TEMP_PATH;
            final String newPath = getSnapshotPath(newIndex);

            if (!destroySnapshot(newPath)) {
                LOG.warn("Delete new snapshot path failed, path is {}.", newPath);
                ret = RaftError.EIO.getNumber();
                break;
            }
            LOG.info("Renaming {} to {}.", tempPath, newPath);
            if (!new File(tempPath).renameTo(new File(newPath))) {
                LOG.error("Renamed temp snapshot failed, from path {} to path {}.", tempPath, newPath);
                ret = RaftError.EIO.getNumber();
                break;
            }
            ref(newIndex);
            this.lock.lock();
            try {
                Requires.requireTrue(oldIndex == this.lastSnapshotIndex);
                this.lastSnapshotIndex = newIndex;
            } finally {
                this.lock.unlock();
            }
            unref(oldIndex);
        } while (false);
        if (ret != 0 && !keepDataOnError) {
            destroySnapshot(writer.getPath());
        }
        if (ret == RaftError.EIO.getNumber()) {
            throw new IOException();
        }
    }

    @Override
    public void shutdown() {
        // ignore
    }

    @Override
    public boolean setFilterBeforeCopyRemote() {
        this.filterBeforeCopyRemote = true;
        return true;
    }

    @Override
    public SnapshotWriter create() {
        return create(true);
    }

    public SnapshotWriter create(final boolean fromEmpty) {
        LocalSnapshotWriter writer = null;
        // noinspection ConstantConditions
        do {
            final String snapshotPath = this.path + File.separator + TEMP_PATH;
            // delete temp
            // TODO: Notify watcher before deleting
            if (new File(snapshotPath).exists() && fromEmpty) {
                if (!destroySnapshot(snapshotPath)) {
                    break;
                }
            }
            writer = new LocalSnapshotWriter(snapshotPath, this, this.raftOptions);
            if (!writer.init(null)) {
                LOG.error("Fail to init snapshot writer.");
                writer = null;
                break;
            }
        } while (false);
        return writer;
    }

    /**
     * 获取一个专门用于读取快照文件的对象  如果快照文件不存在 会返回null
     * @return
     */
    @Override
    public SnapshotReader open() {
        long lsIndex = 0;
        this.lock.lock();
        try {
            // lastSnapshotIndex 代表快照文件的下标而不是偏移量 这样就代表 又有一处在引用该映射文件 (应该是指代reader 在引用该文件)
            if (this.lastSnapshotIndex != 0) {
                lsIndex = this.lastSnapshotIndex;
                ref(lsIndex);
            }
        } finally {
            this.lock.unlock();
        }
        if (lsIndex == 0) {
            LOG.warn("No data for snapshot reader {}.", this.path);
            return null;
        }
        // 生成快照文件名
        final String snapshotPath = getSnapshotPath(lsIndex);
        // 初始化 reader 对象并返回
        final SnapshotReader reader = new LocalSnapshotReader(this, this.snapshotThrottle, this.addr, this.raftOptions,
            snapshotPath);
        // 初始化
        if (!reader.init(null)) {
            LOG.error("Fail to init reader for path {}.", snapshotPath);
            unref(lsIndex);
            return null;
        }
        return reader;
    }

    @Override
    public SnapshotReader copyFrom(final String uri, final SnapshotCopierOptions opts) {
        final SnapshotCopier copier = startToCopyFrom(uri, opts);
        if (copier == null) {
            return null;
        }
        try {
            copier.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Join on snapshot copier was interrupted.");
            return null;
        }
        final SnapshotReader reader = copier.getReader();
        Utils.closeQuietly(copier);
        return reader;
    }

    @Override
    public SnapshotCopier startToCopyFrom(final String uri, final SnapshotCopierOptions opts) {
        final LocalSnapshotCopier copier = new LocalSnapshotCopier();
        copier.setStorage(this);
        copier.setSnapshotThrottle(this.snapshotThrottle);
        copier.setFilterBeforeCopyRemote(this.filterBeforeCopyRemote);
        if (!copier.init(uri, opts)) {
            LOG.error("Fail to init copier to {}.", uri);
            return null;
        }
        copier.start();
        return copier;
    }

}
