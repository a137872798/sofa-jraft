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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.FileSource;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.remote.RemoteFileCopier;
import com.alipay.sofa.jraft.storage.snapshot.remote.Session;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Copy another machine snapshot to local.
 * 从leader将快照信息拷贝到本地(follower)
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-07 11:32:30 AM
 */
public class LocalSnapshotCopier extends SnapshotCopier {

    private static final Logger          LOG  = LoggerFactory.getLogger(LocalSnapshotCopier.class);

    private final Lock                   lock = new ReentrantLock();
    /** The copy job future object
     *  保存一组 拷贝任务的结果
     * */
    private volatile Future<?>           future;
    private boolean                      cancelled;
    /** snapshot writer
     *  写快照对象
     * */
    private LocalSnapshotWriter          writer;
    /** snapshot reader
     *  读快照对象
     * */
    private volatile LocalSnapshotReader reader;
    /** snapshot storage
     *  快照存储对象 应该是通过 reader 和 writer 对象去访问该对象
     * */
    private LocalSnapshotStorage         storage;
    /**
     * 是否要对从远端来的数据进行过滤
     */
    private boolean                      filterBeforeCopyRemote;
    /**
     * 对应远端快照
     */
    private LocalSnapshot                remoteSnapshot;
    /** remote file copier
     *  远端文件 拷贝对象  实际上逻辑都在该对象内
     */
    private RemoteFileCopier             copier;
    /** current copying session
     *  会话对象
     * */
    private Session                      curSession;
    /**
     * 拷贝快照时的过滤器
     */
    private SnapshotThrottle             snapshotThrottle;

    public void setSnapshotThrottle(final SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    /**
     * 开始拷贝
     */
    private void startCopy() {
        try {
            internalCopy();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); //reset/ignore
        } catch (final IOException e) {
            LOG.error("Fail to start copy job", e);
        }
    }

    /**
     * 这里已经是 follower 连接上leader 并开始拉取快照数据了   为什么采用拉模式 而不是 推模式(leader 将数据送到follower上)
     * @throws IOException
     * @throws InterruptedException
     */
    private void internalCopy() throws IOException, InterruptedException {
        // noinspection ConstantConditions
        do {
            // 在准备拉取快照数据前  先拉取元数据 因为里面 包含了 一共有多少映射文件以及文件名等信息
            loadMetaTable();
            if (!isOk()) {
                break;
            }
            // 过滤无效数据并通过writer 写入数据
            filter();
            if (!isOk()) {
                break;
            }
            final Set<String> files = this.remoteSnapshot.listFiles();
            for (final String file : files) {
                // 拷贝文件   这里干嘛又写 一次 上面先将session 的数据写入到 buffer中 这里又写入到文件中 啥意思???
                copyFile(file);
            }
        } while (false);
        // 如果 尝试拉取快照失败
        if (!isOk() && this.writer != null && this.writer.isOk()) {
            this.writer.setError(getCode(), getErrorMsg());
        }
        if (this.writer != null) {
            Utils.closeQuietly(this.writer);
            this.writer = null;
        }
        if (isOk()) {
            // 完成后又打开reader 对象???
            this.reader = (LocalSnapshotReader) this.storage.open();
        }
    }

    void copyFile(final String fileName) throws IOException, InterruptedException {
        // 已存在数据就不拷贝了
        if (this.writer.getFileMeta(fileName) != null) {
            LOG.info("Skipped downloading {}", fileName);
            return;
        }
        final String filePath = this.writer.getPath() + File.separator + fileName;
        // 将string 转换成 文件路径 可以传入多个参数 会拼接在第一个参数上
        final Path subPath = Paths.get(filePath);
        if (!subPath.equals(subPath.getParent()) && !subPath.getParent().getFileName().toString().equals(".")) {
            final File parentDir = subPath.getParent().toFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                LOG.error("Fail to create directory for {}", filePath);
                setError(RaftError.EIO, "Fail to create directory");
                return;
            }
        }

        // 找到对应元数据
        final LocalFileMeta meta = (LocalFileMeta) this.remoteSnapshot.getFileMeta(fileName);
        Session session = null;
        try {
            this.lock.lock();
            try {
                if (this.cancelled) {
                    if (isOk()) {
                        setError(RaftError.ECANCELED, "ECANCELED");
                    }
                    return;
                }
                // 将数据写入到文件中
                session = this.copier.startCopyToFile(fileName, filePath, null);
                if (session == null) {
                    LOG.error("Fail to copy {}", fileName);
                    setError(-1, "Fail to copy %s", fileName);
                    return;
                }
                this.curSession = session;

            } finally {
                this.lock.unlock();
            }
            session.join(); // join out of lock
            this.lock.lock();
            try {
                this.curSession = null;
            } finally {
                this.lock.unlock();
            }
            if (!session.status().isOk() && isOk()) {
                setError(session.status().getCode(), session.status().getErrorMsg());
                return;
            }
            // 将文件映射关系写入 到 writer 中
            if (!this.writer.addFile(fileName, meta)) {
                setError(RaftError.EIO, "Fail to add file to writer");
                return;
            }
            if (!this.writer.sync()) {
                setError(RaftError.EIO, "Fail to sync writer");
            }
        } finally {
            if (session != null) {
                Utils.closeQuietly(session);
            }
        }
    }

    /**
     * 加载元数据表  是保存到 remoteSnapshot
     * @throws InterruptedException
     */
    private void loadMetaTable() throws InterruptedException {
        // 初始化一个空容器对象 不过对象具备自动扩容能力
        final ByteBufferCollector metaBuf = ByteBufferCollector.allocate(0);
        Session session = null;
        try {
            this.lock.lock();
            try {
                // 如果关闭的情况 发现 state为空 设置异常结果
                if (this.cancelled) {
                    // state 为null  或者 code == 0
                    if (isOk()) {
                        // 设置 state 为 被关闭 也就是说 state 只有当 产出结果时 才会设置 作为本次操作的结果
                        setError(RaftError.ECANCELED, "ECANCELED");
                    }
                    return;
                }
                // 将元数据保存到 buffer中
                session = this.copier.startCopy2IoBuffer(Snapshot.JRAFT_SNAPSHOT_META_FILE, metaBuf, null);
                this.curSession = session;
            } finally {
                this.lock.unlock();
            }
            // 阻塞 直到从远端拉取完数据 如果超时会使用一个定时器拉取 这时不会解除闭锁 如果重试次数超过最大限度 返回异常并解除闭锁
            session.join(); //join out of lock.
            this.lock.lock();
            try {
                this.curSession = null;
            } finally {
                this.lock.unlock();
            }
            // session 已经设置了 state 而本对象没有设置 state 那么就将本对象的state 设置成异常情况
            if (!session.status().isOk() && isOk()) {
                LOG.warn("Fail to copy meta file: {}", session.status());
                setError(session.status().getCode(), session.status().getErrorMsg());
                return;
            }
            // 这里从 buffer中取出数据 如果获取失败设置异常
            if (!this.remoteSnapshot.getMetaTable().loadFromIoBufferAsRemote(metaBuf.getBuffer())) {
                LOG.warn("Bad meta_table format");
                setError(-1, "Bad meta_table format from remote");
                return;
            }
            Requires.requireTrue(this.remoteSnapshot.getMetaTable().hasMeta(), "Invalid remote snapshot meta:%s",
                this.remoteSnapshot.getMetaTable().getMeta());
        } finally {
            if (session != null) {
                Utils.closeQuietly(session);
            }
        }
    }

    /**
     * 该方法进入的前提是 writer 已经写入了元数据文件  该方法应该是要剔除掉那些比较奇怪的元数据对应的文件
     * @param writer
     * @param lastSnapshot
     * @return
     * @throws IOException
     */
    boolean filterBeforeCopy(final LocalSnapshotWriter writer, final SnapshotReader lastSnapshot) throws IOException {
        // 当前在 writer 中找到的一组 文件名 代表一共写了哪些快照文件
        final Set<String> existingFiles = writer.listFiles();
        final ArrayDeque<String> toRemove = new ArrayDeque<>();
        for (final String file : existingFiles) {
            // 从快照对应中找到对应的 元数据  找不到 就设置到一个要移除的set 中
            if (this.remoteSnapshot.getFileMeta(file) == null) {
                toRemove.add(file);
                writer.removeFile(file);
            }
        }

        // 获取从 leader 拉取到的快照文件名 列表
        final Set<String> remoteFiles = this.remoteSnapshot.listFiles();

        for (final String fileName : remoteFiles) {
            // 获取对应的元数据
            final LocalFileMeta remoteMeta = (LocalFileMeta) this.remoteSnapshot.getFileMeta(fileName);
            Requires.requireNonNull(remoteMeta, "remoteMeta");
            // 如果该文件没有设置校验和 重新下载
            if (!remoteMeta.hasChecksum()) {
                // Re-download file if this file doesn't have checksum
                writer.removeFile(fileName);
                toRemove.add(fileName);
                continue;
            }

            // 从writer 中找到对应的元数据
            LocalFileMeta localMeta = (LocalFileMeta) writer.getFileMeta(fileName);
            if (localMeta != null) {
                // 代表元数据是正常的
                if (localMeta.hasChecksum() && localMeta.getChecksum().equals(remoteMeta.getChecksum())) {
                    LOG.info("Keep file={} checksum={} in {}", fileName, remoteMeta.getChecksum(), writer.getPath());
                    continue;
                }
                // Remove files from writer so that the file is to be copied from
                // remote_snapshot or last_snapshot
                // 代表 对应的元数据校验和匹配失败
                writer.removeFile(fileName);
                toRemove.add(fileName);
            }
            // Try find files in last_snapshot  如果不存在可以读取的数据
            if (lastSnapshot == null) {
                continue;
            }
            // 如果没有找到指定文件名对应的数据
            if ((localMeta = (LocalFileMeta) lastSnapshot.getFileMeta(fileName)) == null) {
                continue;
            }
            // 读取的数据 对应元数据校验失败 不需要添加到set 中吗
            if (!localMeta.hasChecksum() || !localMeta.getChecksum().equals(remoteMeta.getChecksum())) {
                continue;
            }

            LOG.info("Found the same file ={} checksum={} in lastSnapshot={}", fileName, remoteMeta.getChecksum(),
                lastSnapshot.getPath());
            // 如果source 是本地文件  默认创建的元数据就是这个
            if (localMeta.getSource() == FileSource.FILE_SOURCE_LOCAL) {
                // 找到对应 leader 上的快照文件
                final String sourcePath = lastSnapshot.getPath() + File.separator + fileName;
                final String destPath = writer.getPath() + File.separator + fileName;
                FileUtils.deleteQuietly(new File(destPath));
                try {
                    // 将2个文件连接起来  这里涉及到操作系统的软链概念了
                    Files.createLink(Paths.get(destPath), Paths.get(sourcePath));
                } catch (final IOException e) {
                    LOG.error("Fail to link {} to {}", sourcePath, destPath, e);
                    continue;
                }
                // Don't delete linked file
                if (!toRemove.isEmpty() && toRemove.peekLast().equals(fileName)) {
                    toRemove.pollLast();
                }
            }
            // Copy file from last_snapshot
            writer.addFile(fileName, localMeta);
        }
        // 同步写入失败
        if (!writer.sync()) {
            LOG.error("Fail to sync writer on path={}", writer.getPath());
            return false;
        }
        // 移除 无效数据
        for (final String fileName : toRemove) {
            final String removePath = writer.getPath() + File.separator + fileName;
            FileUtils.deleteQuietly(new File(removePath));
            LOG.info("Deleted file: {}", removePath);
        }
        return true;
    }

    /**
     * 过滤数据
     * @throws IOException
     */
    private void filter() throws IOException {
        // 创建一个专门用于写入快照文件的对象 同时会创建 本地的 元数据文件 而在loadMetaTable() 阶段是将leader 的元数据拉取到 remoteSnapshot中
        // 如果 filterBeforeCopyRemote == true 代表 如果文件已经存在会读取之前的数据
        this.writer = (LocalSnapshotWriter) this.storage.create(!this.filterBeforeCopyRemote);
        // 创建失败 抛出异常
        if (this.writer == null) {
            setError(RaftError.EIO, "Fail to create snapshot writer");
            return;
        }
        // 代表允许读取元数据文件 这样reader 会指向 writer 创建的元数据文件
        if (this.filterBeforeCopyRemote) {
            // 获取读取对象
            final SnapshotReader reader = this.storage.open();
            if (!filterBeforeCopy(this.writer, reader)) {
                LOG.warn("Fail to filter writer before copying, destroy and create a new writer.");
                this.writer.setError(-1, "Fail to filter");
                Utils.closeQuietly(this.writer);
                this.writer = (LocalSnapshotWriter) this.storage.create(true);
            }
            // 这里数据已经读取完了
            if (reader != null) {
                Utils.closeQuietly(reader);
            }
            if (this.writer == null) {
                setError(RaftError.EIO, "Fail to create snapshot writer");
                return;
            }
        }
        // 写入元数据
        this.writer.saveMeta(this.remoteSnapshot.getMetaTable().getMeta());
        // 如果同步写入失败
        if (!this.writer.sync()) {
            LOG.error("Fail to sync snapshot writer path={}", this.writer.getPath());
            setError(RaftError.EIO, "Fail to sync snapshot writer");
        }
    }

    /**
     * 初始化 copier 对象
     * @param uri
     * @param opts
     * @return
     */
    public boolean init(final String uri, final SnapshotCopierOptions opts) {
        this.copier = new RemoteFileCopier();
        this.cancelled = false;
        // 设置阀门属性
        this.filterBeforeCopyRemote = opts.getNodeOptions().isFilterBeforeCopyRemote();
        // 设置远端快照对象
        this.remoteSnapshot = new LocalSnapshot(opts.getRaftOptions());
        // 初始化
        return this.copier.init(uri, this.snapshotThrottle, opts);
    }

    public SnapshotStorage getStorage() {
        return this.storage;
    }

    public void setStorage(final SnapshotStorage storage) {
        this.storage = (LocalSnapshotStorage) storage;
    }

    public boolean isFilterBeforeCopyRemote() {
        return this.filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(final boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

    @Override
    public void close() throws IOException {
        cancel();
        try {
            join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void start() {
        this.future = Utils.runInThread(this::startCopy);
    }

    /**
     * 可能是某个leader 失效了 那么就不能继续从那个节点拉取快照了 就要将本任务关闭
     */
    @Override
    public void cancel() {
        this.lock.lock();
        try {
            if (this.cancelled) {
                return;
            }
            if (isOk()) {
                setError(RaftError.ECANCELED, "Cancel the copier manually.");
            }
            this.cancelled = true;
            if (this.curSession != null) {
                this.curSession.cancel();
            }
            if (this.future != null) {
                this.future.cancel(true);
            }
        } finally {
            this.lock.unlock();
        }

    }

    @Override
    public void join() throws InterruptedException {
        if (this.future != null) {
            try {
                this.future.get();
            } catch (final InterruptedException e) {
                throw e;
            } catch (final CancellationException ignored) {
                // ignored
            } catch (final Exception e) {
                LOG.error("Fail to join on copier", e);
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public SnapshotReader getReader() {
        return this.reader;
    }
}
