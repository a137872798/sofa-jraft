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
 * 该对象是用户与状态机交互的门面 通过将不同的事件 发布到该对象中 包装成 iteratorImpl 后提交到状态机
 * 之后就走raft 那一套
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
        // 本批提交的最大偏移量
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
     * 代表在某个 IteratorImpl 中正在处理的回调对应的 entry写入的下标
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
        // 初始状态下 appliedIndex 为0
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
        // 回调对象被存放到该队列中
        this.closureQueue = opts.getClosureQueue();
        // 该对象关联到 node 中的 afterShutdown
        this.afterShutdown = opts.getAfterShutdown();
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        // BootstrapId index 和 term 都是0
        this.lastAppliedIndex.set(opts.getBootstrapId().getIndex());
        // 触发监听器
        // readOnlyServiceImpl 会在该对象初始化完后 才设置到里面
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
     * 生成一个command 任务
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

    /**
     * 当leader 停止时触发 将任务添加到ringBuffer 中
     * @param status status info
     * @return
     */
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

    /**
     * 当某个刚重置过leader 的follower 又收到某个leader 发来的数据时 重新开始跟随leader
     * @param ctx context of leader change
     * @return
     */
    @Override
    public boolean onStartFollowing(final LeaderChangeContext ctx) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.START_FOLLOWING;
            task.leaderChangeCtx = new LeaderChangeContext(ctx.getLeaderId(), ctx.getTerm(), ctx.getStatus());
        });
    }

    /**
     * 当接收到 leader发生变更 同时follower 不再跟随该leader时触发
     * @param ctx context of leader change
     * @return
     */
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
     * 当发生异常时触发 向 disruptor写入异常事件 当用户发布一个任务时 如果在LogManager 中写入失败就会抛出该异常
     * @param error error info
     * @return
     */
    @Override
    public boolean onError(final RaftException error) {
        // 这里避免异常被重复执行 因为 caller.onError <-> node.onError 会相互调用
        if (!this.error.getStatus().isOk()) {
            LOG.warn("FSMCaller already in error status, ignore new error: {}", error);
            return false;
        }
        // 将异常用一个 特殊的回调对象包装
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

    /**
     * 推测是 这样 一旦发现 shutdownLatch 不为空 代表创建了一个 允许同步调用的任务 那么在完成任务后就会 解除 阻塞 并调用shutdown
     * 注意 join完后就将shutdownLatch 置空了 正对应         if (this.shutdownLatch != null) {
     * @throws InterruptedException
     */
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
     * @param maxCommittedIndex   以批为单位 某次批量任务执行时的最大偏移量 当执行完后 偏移量会重置成0
     *                            同一批的意思是前面endofBatch 都是false
     * @param endOfBatch
     * @return
     */
    @SuppressWarnings("ConstantConditions")
    private long runApplyTask(final ApplyTask task, long maxCommittedIndex, final boolean endOfBatch) {
        CountDownLatch shutdown = null;
        if (task.type == TaskType.COMMITTED) {
            // 如果任务提交的偏移量超过了 目标偏移量 就更新  注意这里并没有直接处理 任务 看来commit是在其他的某个时刻触发的
            // 同时 每次执行完提交任务后 maxCommittedIndex 都会变成-1
            if (task.committedIndex > maxCommittedIndex) {
                maxCommittedIndex = task.committedIndex;
            }
        } else {
            // 如果是其他任务进入的情况 发现了当前 maxCommittedIndex 不为-1 代表必须要进行commit 了
            // 等下 这好像是一种延迟写入 只有当需要获取最新数据的时候才要求写入数据 所以当任务不是 commit 时 代表需要读取数据了
            // 那么这时就将数据写入到LogManger 中
            if (maxCommittedIndex >= 0) {
                // 将当前任务设置成提交任务  看来有些地方需要通过 currTask 来做一些事情
                this.currTask = TaskType.COMMITTED;
                // 执行提交任务就是将 LogEntry 保存到LogManager 中
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
                    // node节点如果配置了快照对象 在定时任务中 会保存快照对象 就会转发到这里
                    case SNAPSHOT_SAVE:
                        // 代表当前正在处理 保存快照的任务
                        this.currTask = TaskType.SNAPSHOT_SAVE;
                        // 如果当前Caller 对象已经出现了异常就不能处理了 通过异常status触发回调
                        // passByStatus 返回true 代表caller对象正常
                        if (passByStatus(task.done)) {
                            // 保存快照  由于需要用户自己实现 简单看作将目标数据写入到了文件中 并在writer中保存了 文件名与 元数据的映射关系
                            // 比如 基于kv 的实现就是 将region 持久化
                            doSnapshotSave((SaveSnapshotClosure) task.done);
                        }
                        break;
                    // 当成功从leader 处拉取完快照后触发
                    case SNAPSHOT_LOAD:
                        this.currTask = TaskType.SNAPSHOT_LOAD;
                        if (passByStatus(task.done)) {
                            // 由用户实现
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
                        // 如果遇到了异常 比如写入logManager 失败
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
            // 代表是该批数据的末尾 且 需要提交数据  进行批量提交
            if (endOfBatch && maxCommittedIndex >= 0) {
                this.currTask = TaskType.COMMITTED;
                // 提交数据后将 maxCommittedIndex 重置
                doCommitted(maxCommittedIndex);
                maxCommittedIndex = -1L; // reset maxCommittedIndex
            }
            this.currTask = TaskType.IDLE;
            return maxCommittedIndex;
        } finally {
            // 如果发现闭锁 在处理完任务后 唤醒可能阻塞的线程
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
     * 触发 当LastAppliedIndex发生变化的监听器  实际上就是 触发了 commited 将leader 的LogEntry 发送到了其他 follower
     *
     * @param lastAppliedIndex
     */
    private void notifyLastAppliedIndexUpdated(final long lastAppliedIndex) {
        for (final LastAppliedLogIndexListener listener : this.lastAppliedLogIndexListeners) {
            listener.onApplied(lastAppliedIndex);
        }
    }

    /**
     * 当用户提交任务 并成功刷盘到半数的节点时触发
     *
     * @param committedIndex
     */
    private void doCommitted(final long committedIndex) {
        if (!this.error.getStatus().isOk()) {
            return;
        }
        // 上次commited 的下标
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        // We can tolerate the disorder of committed_index
        if (lastAppliedIndex >= committedIndex) {
            return;
        }
        final long startMs = Utils.monotonicMs();
        try {
            final List<Closure> closures = new ArrayList<>();
            final List<TaskClosure> taskClosures = new ArrayList<>();
            // 从用户的回调队列中取出对应的回调对象 并触发
            // 返回的是 本次这批回调中最早的回调对应的entry刷盘的下标
            final long firstClosureIndex = this.closureQueue.popClosureUntil(committedIndex, closures, taskClosures);

            // Calls TaskClosure#onCommitted if necessary
            // 如果某些回调是 TaskClosure类型 使用该方法处理 相当于对用户开放的对象
            onTaskCommitted(taskClosures);

            Requires.requireTrue(firstClosureIndex >= 0, "Invalid firstClosureIndex");
            // 将 List<Closure> 包装成迭代器对象  即使数据已经成功刷盘到半数的node 如果leader刷盘失败了 那么会在迭代器对象中
            // 设置一个 error 这样调用 hasNext时会抛出异常
            final IteratorImpl iterImpl = new IteratorImpl(this.fsm, this.logManager, closures, firstClosureIndex,
                    lastAppliedIndex, committedIndex, this.applyingIndex);
            // 等同 hasNext()
            while (iterImpl.isGood()) {
                // 每次调用 next LogEntry 会使得 lastAppliedIndex 向 commitedIndex 靠拢
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
                    // 获取对应的回调对象 并触发  (每个下标对应 queue 中的回调对象)
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
                // 处理 data 类型数据  内部也是委托给 状态机
                doApplyTasks(iterImpl);
            }

            // 代表 数据没有刷盘到leader中 很可能只是成功刷盘到了半数以上的follower中
            if (iterImpl.hasError()) {
                setError(iterImpl.getError());
                // 触发剩下任务对应的回调（以失败方式）
                iterImpl.runTheRestClosureWithError();
            }
            // 最后一次调用next 会使得 currentindex 超过 commitindex 所以这里要-1
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
            // 使用状态机处理   也就是data 类型数据 交由 状态机处理
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
     * 保存快照  并触发回调
     *
     * @param done
     */
    private void doSnapshotSave(final SaveSnapshotClosure done) {
        Requires.requireNonNull(done, "SaveSnapshotClosure is null");
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        // 记录本次生成快照的任期与  偏移量
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
        // 获取当前新节点
        for (final PeerId peer : confEntry.getConf()) {
            metaBuilder.addPeers(peer.toString());
        }
        // 当前旧节点
        if (confEntry.getOldConf() != null) {
            for (final PeerId peer : confEntry.getOldConf()) {
                metaBuilder.addOldPeers(peer.toString());
            }
        }
        // 设置 done 的 meta 属性 并返回 writer
        final SnapshotWriter writer = done.start(metaBuilder.build());
        // 没有找到 写对象 抛出异常
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
     * 当从leader 处下载完快照后触发
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
        // 返回本次拉取完leader的快照文件后的元数据
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
        // 获取当前写入的下标  如果是重启的情况 2个 applied 都是0
        final LogId lastAppliedId = new LogId(this.lastAppliedIndex.get(), this.lastAppliedTerm);
        // 当前生成快照对应的位置 针对首次启动的情况就是读取上次保留的最后一个快照
        final LogId snapshotId = new LogId(meta.getLastIncludedIndex(), meta.getLastIncludedTerm());
        // 如果加载的快照比当前还旧 就不需要处理了
        if (lastAppliedId.compareTo(snapshotId) > 0) {
            done.run(new Status(
                    RaftError.ESTALE,
                    "Loading a stale snapshot last_applied_index=%d last_applied_term=%d snapshot_index=%d snapshot_term=%d",
                    lastAppliedId.getIndex(), lastAppliedId.getTerm(), snapshotId.getIndex(), snapshotId.getTerm()));
            return;
        }
        // 使用状态机处理完快照后的逻辑  一般也就是将文件中的数据 覆盖到当前对象上
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
