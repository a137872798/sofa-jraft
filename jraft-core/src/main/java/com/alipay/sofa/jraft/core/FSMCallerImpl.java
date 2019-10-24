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
package com.alipay.sofa.jraft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.closure.TaskClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.DisruptorMetricSet;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * The finite state machine caller implementation.
 * 该对象用于与状态机交互
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Apr-03 11:12:14 AM
 */
public class FSMCallerImpl implements FSMCaller {

    private static final Logger LOG = LoggerFactory.getLogger(FSMCallerImpl.class);

    /**
     * Task type
     *
     * @author boyan (boyan@alibaba-inc.com)
     * <p>
     * 2018-Apr-03 11:12:25 AM
     */
    private enum TaskType {
        IDLE, //
        COMMITTED, //
        SNAPSHOT_SAVE, //
        SNAPSHOT_LOAD, //
        LEADER_STOP, //
        LEADER_START, //
        START_FOLLOWING, //
        STOP_FOLLOWING, //
        SHUTDOWN, //
        FLUSH, //
        ERROR;

        private String metricName;

        public String metricName() {
            if (this.metricName == null) {
                this.metricName = "fsm-" + name().toLowerCase().replaceAll("_", "-");
            }
            return this.metricName;
        }
    }

    /**
     * Apply task for disruptor.
     * disruptor 队列中的任务对象
     * @author boyan (boyan@alibaba-inc.com)
     * <p>
     * 2018-Apr-03 11:12:35 AM
     */
    private static class ApplyTask {
        /**
         * 代表任务类型
         */
        TaskType type;
        // union fields  提交的 下标
        long committedIndex;
        /**
         * 当前任期
         */
        long term;
        Status status;
        LeaderChangeContext leaderChangeCtx;
        Closure done;
        CountDownLatch shutdownLatch;

        public void reset() {
            this.type = null;
            this.committedIndex = 0;
            this.term = 0;
            this.status = null;
            this.leaderChangeCtx = null;
            this.done = null;
            this.shutdownLatch = null;
        }
    }

    private static class ApplyTaskFactory implements EventFactory<ApplyTask> {

        /**
         * 该方法在填充 ringBuffer 时会调用
         * @return
         */
        @Override
        public ApplyTask newInstance() {
            return new ApplyTask();
        }
    }

    /**
     * 事件处理器
     */
    private class ApplyTaskHandler implements EventHandler<ApplyTask> {
        // max committed index in current batch, reset to -1 every batch
        private long maxCommittedIndex = -1;

        /**
         * @param event
         * @param sequence
         * @param endOfBatch 代表当前 消费者 cursor 是否等同于 生产者 cursor 也就是是否是最后一个任务
         * @throws Exception
         */
        @Override
        public void onEvent(final ApplyTask event, final long sequence, final boolean endOfBatch) throws Exception {
            // 处理任务并更新 commitIndex
            this.maxCommittedIndex = runApplyTask(event, this.maxCommittedIndex, endOfBatch);
        }
    }

    /**
     * 存储LogEntry 的入口
     */
    private LogManager logManager;
    /**
     * 状态机对象
     */
    private StateMachine fsm;
    /**
     * 回调队列对象
     */
    private ClosureQueue closureQueue;
    /**
     * 最后生效的 index
     */
    private final AtomicLong lastAppliedIndex;
    /**
     * 最后生效的任期
     */
    private long lastAppliedTerm;
    /**
     * 当 shutdown 后触发的回调对象
     */
    private Closure afterShutdown;
    /**
     * 本caller 对应的node
     */
    private NodeImpl node;
    /**
     * 当前处理的任务类型
     */
    private volatile TaskType currTask;
    /**
     * 当前正在处理的下标
     */
    private final AtomicLong applyingIndex;
    /**
     * 异常对象
     */
    private volatile RaftException error;
    /**
     * 任务队列
     */
    private Disruptor<ApplyTask> disruptor;
    /**
     * 环形缓冲区
     */
    private RingBuffer<ApplyTask> taskQueue;
    /**
     * 终止闭锁
     */
    private volatile CountDownLatch shutdownLatch;
    private NodeMetrics nodeMetrics;
    /**
     * 存放 偏移量发生变化的 监听器
     */
    private final CopyOnWriteArrayList<LastAppliedLogIndexListener> lastAppliedLogIndexListeners = new CopyOnWriteArrayList<>();

    public FSMCallerImpl() {
        super();
        // 代表当前处在空闲状态
        this.currTask = TaskType.IDLE;
        // 这个好像是快照下标
        this.lastAppliedIndex = new AtomicLong(0);
        this.applyingIndex = new AtomicLong(0);
    }

    /**
     * 开始初始化
     * @param opts
     * @return
     */
    @Override
    public boolean init(final FSMCallerOptions opts) {
        // 从opts 中获取需要的属性
        this.logManager = opts.getLogManager();
        this.fsm = opts.getFsm();
        this.closureQueue = opts.getClosureQueue();
        this.afterShutdown = opts.getAfterShutdown();
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        // 默认的 BootstrapId index 和 term 都是0
        this.lastAppliedIndex.set(opts.getBootstrapId().getIndex());
        // 触发监听器
        notifyLastAppliedIndexUpdated(this.lastAppliedIndex.get());
        this.lastAppliedTerm = opts.getBootstrapId().getTerm();
        // 指定多生产者方式
        this.disruptor = DisruptorBuilder.<ApplyTask>newInstance() //
                // 使用指定的事件工厂
                .setEventFactory(new ApplyTaskFactory()) //
                .setRingBufferSize(opts.getDisruptorBufferSize()) //
                .setThreadFactory(new NamedThreadFactory("JRaft-FSMCaller-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        // 设置处理器
        this.disruptor.handleEventsWith(new ApplyTaskHandler());
        this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.taskQueue = this.disruptor.start();
        if (this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry().register("jraft-fsm-caller-disruptor",
                    new DisruptorMetricSet(this.taskQueue));
        }
        // 创建异常对象
        this.error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_NONE);
        LOG.info("Starts FSMCaller successfully.");
        return true;
    }

    /**
     * 终止 caller 对象
     */
    @Override
    public synchronized void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        LOG.info("Shutting down FSMCaller...");

        if (this.taskQueue != null) {
            final CountDownLatch latch = new CountDownLatch(1);
            enqueueTask((task, sequence) -> {
                // 相当于发布一个 终止事件 该方法会将 task 做下述转换后 交由消费者处理
                task.reset();
                task.type = TaskType.SHUTDOWN;
                task.shutdownLatch = latch;
            });
            this.shutdownLatch = latch;
        }
        // 触发终止
        doShutdown();
    }

    @Override
    public void addLastAppliedLogIndexListener(final LastAppliedLogIndexListener listener) {
        this.lastAppliedLogIndexListeners.add(listener);
    }

    private boolean enqueueTask(final EventTranslator<ApplyTask> tpl) {
        if (this.shutdownLatch != null) {
            // Shutting down
            LOG.warn("FSMCaller is stopped, can not apply new task.");
            return false;
        }
        // 生产者发布事件 并唤醒消费者
        this.taskQueue.publishEvent(tpl);
        return true;
    }

    /**
     * 发布一个 commit 任务  外部的操作 最终都转换成对caller 的调用 更进一步说 就是 通过 disruptor 对象发布事件
     * @param committedIndex committed log indexx
     * @return
     */
    @Override
    public boolean onCommitted(final long committedIndex) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.COMMITTED;
            task.committedIndex = committedIndex;
        });
    }

    /**
     * Flush all events in disruptor.
     * 发布一个 刷盘事件
     */
    @OnlyForTest
    void flush() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        enqueueTask((task, sequence) -> {
            task.type = TaskType.FLUSH;
            // 推测 一旦任务设置了 闭锁 那么 在任务被消费者处理后 就会解除闭锁 主流程代码 通过使用闭锁 实现同步机制
            task.shutdownLatch = latch;
        });
        latch.await();
    }

    /**
     * 添加一个下载快照的任务
     *
     * @param done callback
     * @return
     */
    @Override
    public boolean onSnapshotLoad(final LoadSnapshotClosure done) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.SNAPSHOT_LOAD;
            task.done = done;
        });
    }

    @Override
    public boolean onSnapshotSave(final SaveSnapshotClosure done) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.SNAPSHOT_SAVE;
            task.done = done;
        });
    }

    @Override
    public boolean onLeaderStop(final Status status) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.LEADER_STOP;
            task.status = new Status(status);
        });
    }

    @Override
    public boolean onLeaderStart(final long term) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.LEADER_START;
            task.term = term;
        });
    }

    @Override
    public boolean onStartFollowing(final LeaderChangeContext ctx) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.START_FOLLOWING;
            task.leaderChangeCtx = new LeaderChangeContext(ctx.getLeaderId(), ctx.getTerm(), ctx.getStatus());
        });
    }

    @Override
    public boolean onStopFollowing(final LeaderChangeContext ctx) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.STOP_FOLLOWING;
            task.leaderChangeCtx = new LeaderChangeContext(ctx.getLeaderId(), ctx.getTerm(), ctx.getStatus());
        });
    }

    //  以上都是 外部调用caller 将不同类型任务设置到 ringBuffer中 并唤醒消费者 消费者 根据任务类型走不同请求  其中 task 携带闭锁则实现同步机制 如果携带上下文信息 则从上下文获取需要的属性

    /**
     * Closure runs with an error.
     *
     * @author boyan (boyan@alibaba-inc.com)
     * <p>
     * 2018-Apr-04 2:20:31 PM
     */
    public class OnErrorClosure implements Closure {
        private RaftException error;

        public OnErrorClosure(final RaftException error) {
            super();
            this.error = error;
        }

        public RaftException getError() {
            return this.error;
        }

        public void setError(final RaftException error) {
            this.error = error;
        }

        @Override
        public void run(final Status st) {
        }
    }

    /**
     * 当发生异常时触发 也是修改task 对象 并发布
     * @param error error info
     * @return
     */
    @Override
    public boolean onError(final RaftException error) {
        if (!this.error.getStatus().isOk()) {
            LOG.warn("FSMCaller already in error status, ignore new error: {}", error);
            return false;
        }
        final OnErrorClosure c = new OnErrorClosure(error);
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.ERROR;
            task.done = c;
        });
    }

    @Override
    public long getLastAppliedIndex() {
        return this.lastAppliedIndex.get();
    }

    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
            // 执行 shutdown 时 会判断是否有任务积压 有的话 会全部处理完 之后调用halt 这样 一旦调用barrier.waitFor 发现halt 了 就会抛出异常
            this.disruptor.shutdown();
            this.shutdownLatch = null;
        }
    }

    /**
     * 处理任务
     *
     * @param task
     * @param maxCommittedIndex
     * @param endOfBatch
     * @return
     */
    @SuppressWarnings("ConstantConditions")
    private long runApplyTask(final ApplyTask task, long maxCommittedIndex, final boolean endOfBatch) {
        CountDownLatch shutdown = null;
        // 处理提交任务
        if (task.type == TaskType.COMMITTED) {
            // 更新提交index
            if (task.committedIndex > maxCommittedIndex) {
                maxCommittedIndex = task.committedIndex;
            }
        } else {
            // 如果是其他任务进入的情况 发现了当前 maxCommittedIndex 不为-1 代表必须要进行commit 了
            if (maxCommittedIndex >= 0) {
                // 将当前任务设置成提交任务  看来有些地方需要通过 currTask 来做一些事情
                this.currTask = TaskType.COMMITTED;
                doCommitted(maxCommittedIndex);
                maxCommittedIndex = -1L; // reset maxCommittedIndex
            }
            final long startMs = Utils.monotonicMs();
            try {
                // 根据本次任务类型走不同逻辑
                switch (task.type) {
                    case COMMITTED:
                        Requires.requireTrue(false, "Impossible");
                        break;
                    // 如果是保存快照
                    case SNAPSHOT_SAVE:
                        // 代表当前正在处理 保存快照的任务
                        this.currTask = TaskType.SNAPSHOT_SAVE;
                        // 如果当前Caller 对象已经出现了异常就不能处理了 通过异常status触发回调 返回true 代表caller 对象正常
                        if (passByStatus(task.done)) {
                            doSnapshotSave((SaveSnapshotClosure) task.done);
                        }
                        break;
                    // 如果是加载快照
                    case SNAPSHOT_LOAD:
                        this.currTask = TaskType.SNAPSHOT_LOAD;
                        if (passByStatus(task.done)) {
                            doSnapshotLoad((LoadSnapshotClosure) task.done);
                        }
                        break;

                    // 下面的方法直接委托给状态机

                    // 如果 leader 停止了
                    case LEADER_STOP:
                        this.currTask = TaskType.LEADER_STOP;
                        doLeaderStop(task.status);
                        break;
                    // leader 启动
                    case LEADER_START:
                        this.currTask = TaskType.LEADER_START;
                        doLeaderStart(task.term);
                        break;
                    // 代表当前节点开始跟随leader
                    case START_FOLLOWING:
                        this.currTask = TaskType.START_FOLLOWING;
                        doStartFollowing(task.leaderChangeCtx);
                        break;
                    case STOP_FOLLOWING:
                        this.currTask = TaskType.STOP_FOLLOWING;
                        doStopFollowing(task.leaderChangeCtx);
                        break;
                    case ERROR:
                        this.currTask = TaskType.ERROR;
                        doOnError((OnErrorClosure) task.done);
                        break;
                    case IDLE:
                        Requires.requireTrue(false, "Can't reach here");
                        break;

                        // 下面是 2个阻塞对象
                    case SHUTDOWN:
                        this.currTask = TaskType.SHUTDOWN;
                        shutdown = task.shutdownLatch;
                        break;
                    case FLUSH:
                        this.currTask = TaskType.FLUSH;
                        shutdown = task.shutdownLatch;
                        break;
                }
            } finally {
                this.nodeMetrics.recordLatency(task.type.metricName(), Utils.monotonicMs() - startMs);
            }
        }
        try {
            // 代表是该批数据的末尾 且 需要提交数据
            if (endOfBatch && maxCommittedIndex >= 0) {
                this.currTask = TaskType.COMMITTED;
                // 提交数据后将 maxCommittedIndex 重置
                doCommitted(maxCommittedIndex);
                maxCommittedIndex = -1L; // reset maxCommittedIndex
            }
            this.currTask = TaskType.IDLE;
            return maxCommittedIndex;
        } finally {
            // 等待任务完成
            if (shutdown != null) {
                shutdown.countDown();
            }
        }
    }

    /**
     * 触发终止回调 以及关闭状态机
     */
    private void doShutdown() {
        // 置空是帮助node节点被GC 回收
        if (this.node != null) {
            this.node = null;
        }
        if (this.fsm != null) {
            this.fsm.onShutdown();
        }
        if (this.afterShutdown != null) {
            this.afterShutdown.run(Status.OK());
            this.afterShutdown = null;
        }
    }

    /**
     * 触发 当LastAppliedIndex发生变化的监听器
     *
     * @param lastAppliedIndex
     */
    private void notifyLastAppliedIndexUpdated(final long lastAppliedIndex) {
        for (final LastAppliedLogIndexListener listener : this.lastAppliedLogIndexListeners) {
            listener.onApplied(lastAppliedIndex);
        }
    }

    /**
     * 提交数据 直到指定的偏移量
     *
     * @param committedIndex
     */
    private void doCommitted(final long committedIndex) {
        if (!this.error.getStatus().isOk()) {
            return;
        }
        // 获取当前的快照偏移量
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        // We can tolerate the disorder of committed_index
        if (lastAppliedIndex >= committedIndex) {
            return;
        }
        final long startMs = Utils.monotonicMs();
        try {
            final List<Closure> closures = new ArrayList<>();
            final List<TaskClosure> taskClosures = new ArrayList<>();
            // 从任务队列中 取出截取到目标偏移量的所有任务    返回原先的偏移量
            final long firstClosureIndex = this.closureQueue.popClosureUntil(committedIndex, closures, taskClosures);

            // Calls TaskClosure#onCommitted if necessary
            // 如果某些回调是 TaskClosure类型 使用该方法处理 相当于对用户开放的对象
            onTaskCommitted(taskClosures);

            Requires.requireTrue(firstClosureIndex >= 0, "Invalid firstClosureIndex");
            // 将 List<Closure> 包装成迭代器对象  lastAppliedIndex 是当前写入LogStorage 的下标
            final IteratorImpl iterImpl = new IteratorImpl(this.fsm, this.logManager, closures, firstClosureIndex,
                    lastAppliedIndex, committedIndex, this.applyingIndex);
            // 等同 hasNext()
            while (iterImpl.isGood()) {
                // 获取currentIndex(++lastAppliedIndex) 指向的 LogEntry
                final LogEntry logEntry = iterImpl.entry();
                // LogEntry 分为 data 和configuration
                if (logEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                    if (logEntry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        // 这样就代表 集群配置发生了变化  才有提交的必要
                        if (logEntry.getOldPeers() != null && !logEntry.getOldPeers().isEmpty()) {
                            // Joint stage is not supposed to be noticeable by end users.
                            // 提交配置信息  这个提交好像是由 leader 发往同个raft组中其他节点
                            this.fsm.onConfigurationCommitted(new Configuration(iterImpl.entry().getPeers()));
                        }
                    }
                    // 获取对应的回调对象 并触发
                    if (iterImpl.done() != null) {
                        // For other entries, we have nothing to do besides flush the
                        // pending tasks and run this closure to notify the caller that the
                        // entries before this one were successfully committed and applied.
                        iterImpl.done().run(Status.OK());
                    }
                    // 获取下个 LogEntry
                    iterImpl.next();
                    continue;
                }

                // Apply data task to user state machine
                // 如果LogEntry 是 data 数据 内部会调用next
                doApplyTasks(iterImpl);
            }

            // 代表 committedIndex 前对应的某个实体不存在
            if (iterImpl.hasError()) {
                setError(iterImpl.getError());
                // 触发剩下任务对应的回调（以失败方式）
                iterImpl.runTheRestClosureWithError();
            }
            // 获取当前移动到的偏移量
            final long lastIndex = iterImpl.getIndex() - 1;
            // 获取对应数据的任期
            final long lastTerm = this.logManager.getTerm(lastIndex);
            final LogId lastAppliedId = new LogId(lastIndex, lastTerm);
            // 更新当前确认已提交的index 和任期
            this.lastAppliedIndex.set(committedIndex);
            this.lastAppliedTerm = lastTerm;
            // 设置最后提交的LogId
            this.logManager.setAppliedId(lastAppliedId);
            // 触发回调
            notifyLastAppliedIndexUpdated(committedIndex);
        } finally {
            this.nodeMetrics.recordLatency("fsm-commit", Utils.monotonicMs() - startMs);
        }
    }

    /**
     * 提交 TaskClosure 对象
     *
     * @param closures
     */
    private void onTaskCommitted(final List<TaskClosure> closures) {
        for (int i = 0, size = closures.size(); i < size; i++) {
            final TaskClosure done = closures.get(i);
            done.onCommitted();
        }
    }

    /**
     * 处理 data 数据
     *
     * @param iterImpl
     */
    private void doApplyTasks(final IteratorImpl iterImpl) {
        final IteratorWrapper iter = new IteratorWrapper(iterImpl);
        final long startApplyMs = Utils.monotonicMs();
        // 获取当前 LogEntry 对应下标
        final long startIndex = iter.getIndex();
        try {
            // 使用状态机处理
            this.fsm.onApply(iter);
        } finally {
            this.nodeMetrics.recordLatency("fsm-apply-tasks", Utils.monotonicMs() - startApplyMs);
            this.nodeMetrics.recordSize("fsm-apply-tasks-count", iter.getIndex() - startIndex);
        }
        if (iter.hasNext()) {
            LOG.error("Iterator is still valid, did you return before iterator reached the end?");
        }
        // Try move to next in case that we pass the same log twice.
        iter.next();
    }

    /**
     * 保存快照
     *
     * @param done
     */
    private void doSnapshotSave(final SaveSnapshotClosure done) {
        Requires.requireNonNull(done, "SaveSnapshotClosure is null");
        // 代表最后 成功 commited 的数据下标 应该是基于该下标进行提交
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        // 以最后提交的 index,term 为终点 创建快照对象
        final RaftOutter.SnapshotMeta.Builder metaBuilder = RaftOutter.SnapshotMeta.newBuilder() //
                .setLastIncludedIndex(lastAppliedIndex) //
                .setLastIncludedTerm(this.lastAppliedTerm);
        // 获取指定下标往前的最后一个 描述配置的信息
        final ConfigurationEntry confEntry = this.logManager.getConfiguration(lastAppliedIndex);
        // 如果conf 为空 抛出异常
        if (confEntry == null || confEntry.isEmpty()) {
            LOG.error("Empty conf entry for lastAppliedIndex={}", lastAppliedIndex);
            Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Empty conf entry for lastAppliedIndex=%s",
                    lastAppliedIndex));
            return;
        }
        for (final PeerId peer : confEntry.getConf()) {
            metaBuilder.addPeers(peer.toString());
        }
        if (confEntry.getOldConf() != null) {
            for (final PeerId peer : confEntry.getOldConf()) {
                metaBuilder.addOldPeers(peer.toString());
            }
        }
        // 设置 done 的 meta 属性 并返回 writer
        final SnapshotWriter writer = done.start(metaBuilder.build());
        if (writer == null) {
            done.run(new Status(RaftError.EINVAL, "snapshot_storage create SnapshotWriter failed"));
            return;
        }
        // 使用状态机执行 保存快照
        this.fsm.onSnapshotSave(writer, done);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StateMachine [");
        switch (this.currTask) {
            case IDLE:
                sb.append("Idle");
                break;
            case COMMITTED:
                sb.append("Applying logIndex=").append(this.applyingIndex);
                break;
            case SNAPSHOT_SAVE:
                sb.append("Saving snapshot");
                break;
            case SNAPSHOT_LOAD:
                sb.append("Loading snapshot");
                break;
            case ERROR:
                sb.append("Notifying error");
                break;
            case LEADER_STOP:
                sb.append("Notifying leader stop");
                break;
            case LEADER_START:
                sb.append("Notifying leader start");
                break;
            case START_FOLLOWING:
                sb.append("Notifying start following");
                break;
            case STOP_FOLLOWING:
                sb.append("Notifying stop following");
                break;
            case SHUTDOWN:
                sb.append("Shutting down");
                break;
            default:
                break;
        }
        return sb.append(']').toString();
    }

    /**
     * 加载快照
     *
     * @param done
     */
    private void doSnapshotLoad(final LoadSnapshotClosure done) {
        Requires.requireNonNull(done, "LoadSnapshotClosure is null");
        // 返回done 内部的reader 对象
        final SnapshotReader reader = done.start();
        if (reader == null) {
            done.run(new Status(RaftError.EINVAL, "open SnapshotReader failed"));
            return;
        }
        // 通过reader 对象去加载快照
        final RaftOutter.SnapshotMeta meta = reader.load();
        if (meta == null) {
            done.run(new Status(RaftError.EINVAL, "SnapshotReader load meta failed"));
            if (reader.getRaftError() == RaftError.EIO) {
                final RaftException err = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT, RaftError.EIO,
                        "Fail to load snapshot meta");
                setError(err);
            }
            return;
        }
        // 代表当前的快照信息
        final LogId lastAppliedId = new LogId(this.lastAppliedIndex.get(), this.lastAppliedTerm);
        final LogId snapshotId = new LogId(meta.getLastIncludedIndex(), meta.getLastIncludedTerm());
        // 如果加载的快照比当前还旧 就不需要处理了
        if (lastAppliedId.compareTo(snapshotId) > 0) {
            done.run(new Status(
                    RaftError.ESTALE,
                    "Loading a stale snapshot last_applied_index=%d last_applied_term=%d snapshot_index=%d snapshot_term=%d",
                    lastAppliedId.getIndex(), lastAppliedId.getTerm(), snapshotId.getIndex(), snapshotId.getTerm()));
            return;
        }
        // 使用状态机加载数据时抛出异常
        if (!this.fsm.onSnapshotLoad(reader)) {
            done.run(new Status(-1, "StateMachine onSnapshotLoad failed"));
            final RaftException e = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE,
                    RaftError.ESTATEMACHINE, "StateMachine onSnapshotLoad failed");
            setError(e);
            return;
        }
        if (meta.getOldPeersCount() == 0) {
            // Joint stage is not supposed to be noticeable by end users.
            final Configuration conf = new Configuration();
            for (int i = 0, size = meta.getPeersCount(); i < size; i++) {
                final PeerId peer = new PeerId();
                Requires.requireTrue(peer.parse(meta.getPeers(i)), "Parse peer failed");
                conf.addPeer(peer);
            }
            // 使用状态机提交配置
            this.fsm.onConfigurationCommitted(conf);
        }
        // 更新成快照的值
        this.lastAppliedIndex.set(meta.getLastIncludedIndex());
        this.lastAppliedTerm = meta.getLastIncludedTerm();
        done.run(Status.OK());
    }

    private void doOnError(final OnErrorClosure done) {
        setError(done.getError());
    }

    /**
     * 使用状态机处理 leader 暂停的情况
     *
     * @param status
     */
    private void doLeaderStop(final Status status) {
        this.fsm.onLeaderStop(status);
    }

    /**
     * 当leader 启动时触发
     *
     * @param term
     */
    private void doLeaderStart(final long term) {
        this.fsm.onLeaderStart(term);
    }

    private void doStartFollowing(final LeaderChangeContext ctx) {
        this.fsm.onStartFollowing(ctx);
    }

    private void doStopFollowing(final LeaderChangeContext ctx) {
        this.fsm.onStopFollowing(ctx);
    }

    /**
     * 设置结果 并用状态机和 节点去处理
     *
     * @param e
     */
    private void setError(final RaftException e) {
        if (this.error.getType() != EnumOutter.ErrorType.ERROR_TYPE_NONE) {
            // already report
            return;
        }
        this.error = e;
        if (this.fsm != null) {
            this.fsm.onError(e);
        }
        if (this.node != null) {
            this.node.onError(e);
        }
    }

    @OnlyForTest
    RaftException getError() {
        return this.error;
    }

    private boolean passByStatus(final Closure done) {
        final Status status = this.error.getStatus();
        if (!status.isOk()) {
            if (done != null) {
                done.run(new Status(RaftError.EINVAL, "FSMCaller is in bad status=`%s`", status));
                return false;
            }
        }
        return true;
    }

    @Override
    public void describe(final Printer out) {
        out.print("  ") //
                .println(toString());
    }
}
