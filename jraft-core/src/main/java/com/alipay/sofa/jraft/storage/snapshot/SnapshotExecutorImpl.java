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
package com.alipay.sofa.jraft.storage.snapshot;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.CountDownEvent;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Snapshot executor implementation.
 * 可以看作是一个快照模块的中枢  连接了 node 与snapshot 相关组件
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 5:38:56 PM
 */
public class SnapshotExecutorImpl implements SnapshotExecutor {

    private static final Logger                        LOG                 = LoggerFactory
                                                                               .getLogger(SnapshotExecutorImpl.class);

    /**
     * JVM 锁
     */
    private final Lock                                 lock                = new ReentrantLock();

    /**
     * 最后一次快照的任期
     */
    private long                                       lastSnapshotTerm;
    /**
     * 最后快照的下标
     */
    private long                                       lastSnapshotIndex;
    /**
     * 当前任期
     */
    private long                                       term;
    /**
     * 是否正在保存快照中
     */
    private volatile boolean                           savingSnapshot;
    /**
     * 正在加载快照
     */
    private volatile boolean                           loadingSnapshot;
    /**
     * 已经停止
     */
    private volatile boolean                           stopped;
    private SnapshotStorage                            snapshotStorage;
    /**
     * 当前拷贝对象
     */
    private SnapshotCopier                             curCopier;
    /**
     * 状态机调用者
     */
    private FSMCaller                                  fsmCaller;
    /**
     * 节点
     */
    private NodeImpl                                   node;
    private LogManager                                 logManager;
    private final AtomicReference<DownloadingSnapshot> downloadingSnapshot = new AtomicReference<>(null);
    /**
     * 快照元数据信息
     */
    private SnapshotMeta                               loadingSnapshotMeta;
    /**
     * 简单的加工一下闭锁对象
     */
    private final CountDownEvent                       runningJobs         = new CountDownEvent();

    /**
     * Downloading snapshot job.
     * 代表正在下载快照的任务
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:07:19 PM
     */
    static class DownloadingSnapshot {
        /**
         * 安装快照请求
         */
        InstallSnapshotRequest          request;
        /**
         * 安装快照响应构建器
         */
        InstallSnapshotResponse.Builder responseBuilder;
        RpcRequestClosure               done;

        public DownloadingSnapshot(final InstallSnapshotRequest request,
                                   final InstallSnapshotResponse.Builder responseBuilder, final RpcRequestClosure done) {
            super();
            this.request = request;
            this.responseBuilder = responseBuilder;
            this.done = done;
        }

    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotTerm() {
        return this.lastSnapshotTerm;
    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotIndex() {
        return this.lastSnapshotIndex;
    }

    /**
     * Save snapshot done closure
     * 用于保存快照的回调对象
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:07:52 PM
     */
    private class SaveSnapshotDone implements SaveSnapshotClosure {

        /**
         * 写快照对象
         */
        SnapshotWriter writer;
        Closure        done;
        /**
         * 快照元数据
         */
        SnapshotMeta   meta;

        public SaveSnapshotDone(final SnapshotWriter writer, final Closure done, final SnapshotMeta meta) {
            super();
            this.writer = writer;
            this.done = done;
            this.meta = meta;
        }

        @Override
        public void run(final Status status) {
            Utils.runInThread(() -> continueRun(status));
        }

        /**
         * 按照传入的 结果来处理 保存快照的回调
         * @param st
         */
        void continueRun(final Status st) {
            // 将结果保存到 LogManager 中
            final int ret = onSnapshotSaveDone(st, this.meta, this.writer);
            if (ret != 0 && st.isOk()) {
                st.setError(ret, "node call onSnapshotSaveDone failed");
            }
            // 如果该对象内部包含回调对象 传播触发回调
            if (this.done != null) {
                Utils.runClosureInThread(this.done, st);
            }
        }

        /**
         * 设置 meta 属性 并返回 writer 对象
         * @param meta metadata of snapshot.
         * @return
         */
        @Override
        public SnapshotWriter start(final SnapshotMeta meta) {
            this.meta = meta;
            return this.writer;

        }

    }

    /**
     * Install snapshot done closure
     * 安装快照的回调对象
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:08:09 PM
     */
    private class InstallSnapshotDone implements LoadSnapshotClosure {

        /**
         * 读取快照对象
         */
        SnapshotReader reader;

        public InstallSnapshotDone(final SnapshotReader reader) {
            super();
            this.reader = reader;
        }

        @Override
        public void run(final Status status) {
            // 触发安装快照
            onSnapshotLoadDone(status);
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }

    }

    /**
     * Load snapshot at first time closure
     * @author boyan (boyan@alibaba-inc.com)
     * 当首次开始加载快照时触发  该对象相比上面的 就是多了一个闭锁
     * 首次加载快照时 必然要阻塞完成
     * 2018-Apr-16 2:57:46 PM
     */
    private class FirstSnapshotLoadDone implements LoadSnapshotClosure {

        /**
         * 用于读取快照的reader 对象  或者说该对象连接到了快照文件
         */
        SnapshotReader reader;
        /**
         * 栅栏
         */
        CountDownLatch eventLatch;
        /**
         * 描述结果的对象 内含code 和 msg
         */
        Status         status;

        /**
         * 通过一个 reader 对象进行初始化
         * @param reader
         */
        public FirstSnapshotLoadDone(final SnapshotReader reader) {
            super();
            this.reader = reader;
            this.eventLatch = new CountDownLatch(1);
        }

        /**
         * 当任务完成时 触发
         * @param status the task status. 任务结果
         */
        @Override
        public void run(final Status status) {
            this.status = status;
            // 设置加载结果
            onSnapshotLoadDone(this.status);
            // 这个就是配合 wait 等待加载结束
            this.eventLatch.countDown();
        }

        public void waitForRun() throws InterruptedException {
            this.eventLatch.await();
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }

    }

    /**
     * 使用指定的 opts 来初始化 executor对象
     * @param opts
     * @return
     */
    @Override
    public boolean init(final SnapshotExecutorOptions opts) {
        if (StringUtils.isBlank(opts.getUri())) {
            LOG.error("Snapshot uri is empty");
            return false;
        }
        this.logManager = opts.getLogManager();
        this.fsmCaller = opts.getFsmCaller();
        this.node = opts.getNode();
        this.term = opts.getInitTerm();
        // 创建快照存储对象    看来这个url 就是leader 的地址了 这样获取的快照信息才有意义
        this.snapshotStorage = this.node.getServiceFactory().createSnapshotStorage(opts.getUri(),
            this.node.getRaftOptions());
        if (opts.isFilterBeforeCopyRemote()) {
            this.snapshotStorage.setFilterBeforeCopyRemote();
        }
        if (opts.getSnapshotThrottle() != null) {
            this.snapshotStorage.setSnapshotThrottle(opts.getSnapshotThrottle());
        }
        // 初始化 storage 对象 就是删除多余的快照文件 并给最后一个 快照文件引用数 + 1
        if (!this.snapshotStorage.init(null)) {
            LOG.error("Fail to init snapshot storage.");
            return false;
        }
        final LocalSnapshotStorage tmp = (LocalSnapshotStorage) this.snapshotStorage;
        if (tmp != null && !tmp.hasServerAddr()) {
            tmp.setServerAddr(opts.getAddr());
        }
        // 开启快照读取对象  open 方法同时会初始化reader  如果当前没有快照文件 reader 就是null 代表提前退出初始化
        // 如果是首次启动应该是没有快照文件的 那么 reader 就无法成功创建  但是还是任务 init是执行成功的
        final SnapshotReader reader = this.snapshotStorage.open();
        if (reader == null) {
            return true;
        }
        // 有快照文件却没有元数据抛出异常
        this.loadingSnapshotMeta = reader.load();
        if (this.loadingSnapshotMeta == null) {
            LOG.error("Fail to load meta from {}.", opts.getUri());
            Utils.closeQuietly(reader);
            return false;
        }
        // 设置成正在加载快照
        this.loadingSnapshot = true;
        // 增加当前正在执行的任务数量
        this.runningJobs.incrementAndGet();
        // 使用reader 对象创建 一个 当首次加载快照完成时触发的回调对象
        final FirstSnapshotLoadDone done = new FirstSnapshotLoadDone(reader);
        // 使用状态机执行加载快照的任务
        Requires.requireTrue(this.fsmCaller.onSnapshotLoad(done));
        try {
            // 阻塞直到快照加载完成
            done.waitForRun();
        } catch (final InterruptedException e) {
            LOG.warn("Wait for FirstSnapshotLoadDone run is interrupted.");
            Thread.currentThread().interrupt();
            return false;
        } finally {
            Utils.closeQuietly(reader);
        }
        if (!done.status.isOk()) {
            LOG.error("Fail to load snapshot from {},FirstSnapshotLoadDone status is {}", opts.getUri(), done.status);
            return false;
        }
        return true;
    }

    /**
     * 终止快照中枢
     */
    @Override
    public void shutdown() {
        long savedTerm;
        this.lock.lock();
        try {
            savedTerm = this.term;
            this.stopped = true;
        } finally {
            this.lock.unlock();
        }
        // 打断从某个 leader 拉取快照的任务
        interruptDownloadingSnapshots(savedTerm);
    }

    @Override
    public NodeImpl getNode() {
        return this.node;
    }

    @Override
    public void doSnapshot(final Closure done) {
        boolean doUnlock = true;
        this.lock.lock();
        try {
            // 如果当前快照对象已经被关闭
            if (this.stopped) {
                Utils.runClosureInThread(done, new Status(RaftError.EPERM, "Is stopped."));
                return;
            }
            // 如果当前正在下载快照
            if (this.downloadingSnapshot.get() != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is loading another snapshot."));
                return;
            }
            // 如果当前正在保存快照
            if (this.savingSnapshot) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is saving another snapshot."));
                return;
            }

            // 这里代表不需要处理快照
            if (this.fsmCaller.getLastAppliedIndex() == this.lastSnapshotIndex) {
                // There might be false positive as the getLastAppliedIndex() is being
                // updated. But it's fine since we will do next snapshot saving in a
                // predictable time.
                doUnlock = false;
                this.lock.unlock();
                // 将lastSnapshotIndex 之前的数据都清空
                this.logManager.clearBufferedLogs();
                Utils.runClosureInThread(done);
                return;
            }
            // writer 会对应到一个 temp 路径 快照数据首先会写入到 temp中之后 如果调用了close 那么数据会被转移到 真正的快照文件中
            final SnapshotWriter writer = this.snapshotStorage.create();
            if (writer == null) {
                Utils.runClosureInThread(done, new Status(RaftError.EIO, "Fail to create writer."));
                reportError(RaftError.EIO.getNumber(), "Fail to create snapshot writer.");
                return;
            }
            // 代表当前开始保存快照信息
            this.savingSnapshot = true;
            // 创建一个保存快照的回调对象 同时该对象内部嵌套着本次传入的新回调对象
            final SaveSnapshotDone saveSnapshotDone = new SaveSnapshotDone(writer, done, null);
            if (!this.fsmCaller.onSnapshotSave(saveSnapshotDone)) {
                Utils.runClosureInThread(done, new Status(RaftError.EHOSTDOWN, "The raft node is down."));
                return;
            }
            // 这里代表 正在运行中的任务数量增加了1  而在 StateMachine 执行完对应任务后 会将任务数量减少
            this.runningJobs.incrementAndGet();
        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }

    }

    /**
     * 当某次快照存储完毕后触发的回调对象
     * @param st 当前状态
     * @param meta 元数据信息
     * @param writer 将数据写入到指定文件中的writer
     * @return
     */
    int onSnapshotSaveDone(final Status st, final SnapshotMeta meta, final SnapshotWriter writer) {
        int ret;
        this.lock.lock();
        try {
            ret = st.getCode();
            // InstallSnapshot can break SaveSnapshot, check InstallSnapshot when SaveSnapshot
            // because upstream Snapshot maybe newer than local Snapshot.
            if (st.isOk()) {
                // 如果要安装的数据比当前数据还旧  设置异常结果
                if (meta.getLastIncludedIndex() <= this.lastSnapshotIndex) {
                    ret = RaftError.ESTALE.getNumber();
                    if (this.node != null) {
                        LOG.warn("Node {} discards an stale snapshot lastIncludedIndex={}, lastSnapshotIndex={}",
                            this.node.getNodeId(), meta.getLastIncludedIndex(), this.lastSnapshotIndex);
                    }
                    writer.setError(RaftError.ESTALE, "Installing snapshot is older than local snapshot");
                }
            }
        } finally {
            this.lock.unlock();
        }

        if (ret == 0) {
            // 存储元数据到writer中 (实际上是保存到 metaTable中)
            if (!writer.saveMeta(meta)) {
                LOG.warn("Fail to save snapshot {}", writer.getPath());
                ret = RaftError.EIO.getNumber();
            }
        } else {
            if (writer.isOk()) {
                writer.setError(ret, "Fail to do snapshot.");
            }
        }
        try {
            // 将数据从临时文件转移到 目标文件并销毁临时文件
            writer.close();
        } catch (final IOException e) {
            LOG.error("Fail to close writer", e);
            ret = RaftError.EIO.getNumber();
        }
        boolean doUnlock = true;
        this.lock.lock();
        try {
            if (ret == 0) {
                this.lastSnapshotIndex = meta.getLastIncludedIndex();
                this.lastSnapshotTerm = meta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                // 通过LogManager 来保存快照  这里将数据保存到了 ConfigurationManager 并且清除了 过期的 本地LogEntry 复制数据
                this.logManager.setSnapshot(meta); //should be out of lock
                doUnlock = true;
                this.lock.lock();
            }
            if (ret == RaftError.EIO.getNumber()) {
                reportError(RaftError.EIO.getNumber(), "Fail to save snapshot.");
            }
            // 代表保存快照阶段结束
            this.savingSnapshot = false;
            this.runningJobs.countDown();
            return ret;

        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
    }

    /**
     * 当加载快照完成时触发  整体上来说 就是将LogStorage 中的 数据向 快照对齐
     * 这个方法应该是从本地按照快照 而 saveSnapshot 代表从远端拉取快照信息并设置
     * @param st  代表本次结果
     */
    private void onSnapshotLoadDone(final Status st) {
        DownloadingSnapshot m;
        boolean doUnlock = true;
        this.lock.lock();
        try {
            // 必须要在 正在加载快照的状态下才能继续
            Requires.requireTrue(this.loadingSnapshot, "Not loading snapshot");
            // 默认情况下该值应该是null
            m = this.downloadingSnapshot.get();
            // 代表reader  load数据成功
            if (st.isOk()) {
                this.lastSnapshotIndex = this.loadingSnapshotMeta.getLastIncludedIndex();
                this.lastSnapshotTerm = this.loadingSnapshotMeta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                // 设置快照数据
                this.logManager.setSnapshot(this.loadingSnapshotMeta); //should be out of lock
                doUnlock = true;
                this.lock.lock();
            }
            final StringBuilder sb = new StringBuilder();
            if (this.node != null) {
                sb.append("Node ").append(this.node.getNodeId()).append(" ");
            }
            sb.append("onSnapshotLoadDone, ").append(this.loadingSnapshotMeta);
            LOG.info(sb.toString());
            doUnlock = false;
            this.lock.unlock();
            if (this.node != null) {
                // 更新配置  就是将confEntry 同步到 ConfigurationManager上
                this.node.updateConfigurationAfterInstallingSnapshot();
            }
            doUnlock = true;
            this.lock.lock();
            // 代表本地加载快照的动作已经结束了
            this.loadingSnapshot = false;
            this.downloadingSnapshot.set(null);

        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
        // 如果正在下载快照中  TODO 这里还需要再梳理一下
        if (m != null) {
            // Respond RPC
            if (!st.isOk()) {
                m.done.run(st);
            } else {
                m.responseBuilder.setSuccess(true);
                m.done.sendResponse(m.responseBuilder.build());
            }
        }
        // 减少当前执行的任务数量
        this.runningJobs.countDown();
    }

    /**
     * 开始安装快照
     * @param request
     * @param response
     * @param done
     */
    @Override
    public void installSnapshot(final InstallSnapshotRequest request, final InstallSnapshotResponse.Builder response,
                                final RpcRequestClosure done) {
        final SnapshotMeta meta = request.getMeta();
        // 生成一个 下载快照对象的请求
        final DownloadingSnapshot ds = new DownloadingSnapshot(request, response, done);
        //DON'T access request, response, and done after this point
        //as the retry snapshot will replace this one.
        // 将下载快照的任务注册到 本对象上
        if (!registerDownloadingSnapshot(ds)) {
            LOG.warn("Fail to register downloading snapshot");
            // This RPC will be responded by the previous session
            return;
        }
        Requires.requireNonNull(this.curCopier, "curCopier");
        try {
            // 等待 copier 对象下载完数据
            this.curCopier.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Install snapshot copy job was canceled.");
            return;
        }

        loadDownloadingSnapshot(ds, meta);
    }

    /**
     * 加载下载完成的快照
     * @param ds
     * @param meta
     */
    void loadDownloadingSnapshot(final DownloadingSnapshot ds, final SnapshotMeta meta) {
        SnapshotReader reader;
        this.lock.lock();
        try {
            if (ds != this.downloadingSnapshot.get()) {
                //It is interrupted and response by other request,just return
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            reader = this.curCopier.getReader();
            // 以异常方式触发回调
            if (!this.curCopier.isOk()) {
                if (this.curCopier.getCode() == RaftError.EIO.getNumber()) {
                    reportError(this.curCopier.getCode(), this.curCopier.getErrorMsg());
                }
                Utils.closeQuietly(reader);
                ds.done.run(this.curCopier);
                Utils.closeQuietly(this.curCopier);
                this.curCopier = null;
                this.downloadingSnapshot.set(null);
                this.runningJobs.countDown();
                return;
            }
            Utils.closeQuietly(this.curCopier);
            this.curCopier = null;
            if (reader == null || !reader.isOk()) {
                Utils.closeQuietly(reader);
                this.downloadingSnapshot.set(null);
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINTERNAL,
                    "Fail to copy snapshot from %s", ds.request.getUri()));
                this.runningJobs.countDown();
                return;
            }
            this.loadingSnapshot = true;
            this.loadingSnapshotMeta = meta;
        } finally {
            this.lock.unlock();
        }
        // 创建一个 安装快照回调 并触发
        final InstallSnapshotDone installSnapshotDone = new InstallSnapshotDone(reader);
        if (!this.fsmCaller.onSnapshotLoad(installSnapshotDone)) {
            LOG.warn("Fail to  call fsm onSnapshotLoad");
            installSnapshotDone.run(new Status(RaftError.EHOSTDOWN, "This raft node is down"));
        }
    }

    /**
     * 注册下载快照的任务
     */
    @SuppressWarnings("all")
    boolean registerDownloadingSnapshot(final DownloadingSnapshot ds) {
        DownloadingSnapshot saved = null;
        boolean result = true;

        this.lock.lock();
        try {
            // 如果当前 executor 已经终止 直接以 Shutdown 形式触发ds
            if (this.stopped) {
                LOG.warn("Register DownloadingSnapshot failed: node is stopped");
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EHOSTDOWN, "Node is stopped."));
                return false;
            }
            // 如果当前正在保存快照 提示繁忙 这里的意思是 下载和 保存不能同时执行吗???  为什么要这样设计 同时这2者可能会同时触发么
            if (this.savingSnapshot) {
                LOG.warn("Register DownloadingSnapshot failed: is saving snapshot");
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EBUSY, "Node is saving snapshot."));
                return false;
            }

            // 如果当前任期 与请求的任期 不匹配 返回失败信息
            ds.responseBuilder.setTerm(this.term);
            if (ds.request.getTerm() != this.term) {
                LOG.warn("Register DownloadingSnapshot failed: term mismatch, expect {} but {}.", this.term,
                    ds.request.getTerm());
                ds.responseBuilder.setSuccess(false);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            if (ds.request.getMeta().getLastIncludedIndex() <= this.lastSnapshotIndex) {
                LOG.warn(
                    "Register DownloadingSnapshot failed: snapshot is not newer, request lastIncludedIndex={}, lastSnapshotIndex={}.",
                    ds.request.getMeta().getLastIncludedIndex(), this.lastSnapshotIndex);
                ds.responseBuilder.setSuccess(true);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            final DownloadingSnapshot m = this.downloadingSnapshot.get();
            if (m == null) {
                // 设置正在下载快照的任务
                this.downloadingSnapshot.set(ds);
                Requires.requireTrue(this.curCopier == null, "Current copier is not null");
                // 生成session 对象 并开始拉取数据
                this.curCopier = this.snapshotStorage.startToCopyFrom(ds.request.getUri(), newCopierOpts());
                if (this.curCopier == null) {
                    this.downloadingSnapshot.set(null);
                    LOG.warn("Register DownloadingSnapshot failed: fail to copy file from {}", ds.request.getUri());
                    ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to copy from: %s",
                        ds.request.getUri()));
                    return false;
                }
                this.runningJobs.incrementAndGet();
                return true;
            }

            // A previous snapshot is under installing, check if this is the same
            // snapshot and resume it, otherwise drop previous snapshot as this one is
            // newer

            // 这里代表已经存在一个下载任务了  且他们请求的偏移量相同
            if (m.request.getMeta().getLastIncludedIndex() == ds.request.getMeta().getLastIncludedIndex()) {
                // m is a retry
                // Copy |*ds| to |*m| so that the former session would respond
                // this RPC.
                saved = m;
                this.downloadingSnapshot.set(ds);
                // 这种情况返回false
                result = false;
            // 代表本次请求的任务是一个无效的任务
            } else if (m.request.getMeta().getLastIncludedIndex() > ds.request.getMeta().getLastIncludedIndex()) {
                // |is| is older
                LOG.warn("Register DownloadingSnapshot failed:  is installing a newer one, lastIncludeIndex={}",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINVAL,
                    "A newer snapshot is under installing"));
                return false;
            } else {
                // |is| is newer
                // 如果旧的任务已经开始下载了 这里也无法完成注册
                if (this.loadingSnapshot) {
                    LOG.warn("Register DownloadingSnapshot failed: is loading an older snapshot, lastIncludeIndex={}",
                        m.request.getMeta().getLastIncludedIndex());
                    ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EBUSY,
                        "A former snapshot is under loading"));
                    return false;
                }
                // 这里也要关闭 ???  这里不是添加了一个更新的任务吗
                Requires.requireNonNull(this.curCopier, "curCopier");
                this.curCopier.cancel();
                LOG.warn(
                    "Register DownloadingSnapshot failed:an older snapshot is under installing, cancel downloading, lastIncludeIndex={}",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EBUSY,
                    "A former snapshot is under installing, trying to cancel"));
                return false;
            }
        } finally {
            this.lock.unlock();
        }
        // 触发回调
        if (saved != null) {
            // Respond replaced session
            LOG.warn("Register DownloadingSnapshot failed:  interrupted by retry installling request.");
            saved.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINTR,
                "Interrupted by the retry InstallSnapshotRequest"));
        }
        return result;
    }

    private SnapshotCopierOptions newCopierOpts() {
        final SnapshotCopierOptions copierOpts = new SnapshotCopierOptions();
        copierOpts.setNodeOptions(this.node.getOptions());
        copierOpts.setRaftClientService(this.node.getRpcService());
        copierOpts.setTimerManager(this.node.getTimerManager());
        copierOpts.setRaftOptions(this.node.getRaftOptions());
        return copierOpts;
    }

    /**
     * 代表在拉取哪个任期的快照时出现文件  任期即对应某个leader
     * @param newTerm new term num
     */
    @Override
    public void interruptDownloadingSnapshots(final long newTerm) {
        this.lock.lock();
        try {
            Requires.requireTrue(newTerm >= this.term);
            this.term = newTerm;
            // 如果当前没有下载快照 直接返回
            if (this.downloadingSnapshot.get() == null) {
                return;
            }
            // 如果正在加载快照 不能进行打断
            if (this.loadingSnapshot) {
                // We can't interrupt loading
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            // 从远端下载快照实际上就是通过 curCopier 实现的 这里关闭了从远端拉取数据的任务
            this.curCopier.cancel();
            LOG.info("Trying to cancel downloading snapshot: {}", this.downloadingSnapshot.get().request);
        } finally {
            this.lock.unlock();
        }
    }

    private void reportError(final int errCode, final String fmt, final Object... args) {
        final RaftException error = new RaftException(ErrorType.ERROR_TYPE_SNAPSHOT);
        error.setStatus(new Status(errCode, fmt, args));
        this.fsmCaller.onError(error);
    }

    /**
     * 看来 安装快照对应从远端下载快照信息   load 快照 代表从本地快照文件中加载数据
     * @return
     */
    @Override
    public boolean isInstallingSnapshot() {
        return this.downloadingSnapshot.get() != null;
    }

    @Override
    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    @Override
    public void join() throws InterruptedException {
        this.runningJobs.await();
    }

    @Override
    public void describe(final Printer out) {
        final long _lastSnapshotTerm;
        final long _lastSnapshotIndex;
        final long _term;
        final boolean _savingSnapshot;
        final boolean _loadingSnapshot;
        final boolean _stopped;
        this.lock.lock();
        try {
            _lastSnapshotTerm = this.lastSnapshotTerm;
            _lastSnapshotIndex = this.lastSnapshotIndex;
            _term = this.term;
            _savingSnapshot = this.savingSnapshot;
            _loadingSnapshot = this.loadingSnapshot;
            _stopped = this.stopped;
        } finally {
            this.lock.unlock();
        }
        out.print("  lastSnapshotTerm: ") //
            .println(_lastSnapshotTerm);
        out.print("  lastSnapshotIndex: ") //
            .println(_lastSnapshotIndex);
        out.print("  term: ") //
            .println(_term);
        out.print("  savingSnapshot: ") //
            .println(_savingSnapshot);
        out.print("  loadingSnapshot: ") //
            .println(_loadingSnapshot);
        out.print("  stopped: ") //
            .println(_stopped);
    }
}
