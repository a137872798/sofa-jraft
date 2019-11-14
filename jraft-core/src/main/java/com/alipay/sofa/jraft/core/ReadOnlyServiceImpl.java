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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.FSMCaller.LastAppliedLogIndexListener;
import com.alipay.sofa.jraft.ReadOnlyService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.ReadIndexState;
import com.alipay.sofa.jraft.entity.ReadIndexStatus;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.util.Bytes;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.DisruptorMetricSet;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.ThreadHelper;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ZeroByteStringHelper;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Read-only service implementation.
 * 只读服务  包含线性一致读相关逻辑 TODO 稍后看
 * @author dennis
 */
public class ReadOnlyServiceImpl implements ReadOnlyService, LastAppliedLogIndexListener {

    private static final int                           MAX_ADD_REQUEST_RETRY_TIMES = 3;
    /** Disruptor to run readonly service. */
    private Disruptor<ReadIndexEvent>                  readIndexDisruptor;
    private RingBuffer<ReadIndexEvent>                 readIndexQueue;
    private RaftOptions                                raftOptions;
    /**
     * 内部存放节点实现类
     */
    private NodeImpl                                   node;
    private final Lock                                 lock                        = new ReentrantLock();
    /**
     * 状态机对象
     */
    private FSMCaller                                  fsmCaller;
    private volatile CountDownLatch                    shutdownLatch;

    private ScheduledExecutorService                   scheduledExecutorService;

    private NodeMetrics                                nodeMetrics;

    // <logIndex, statusList>
    private final TreeMap<Long, List<ReadIndexStatus>> pendingNotifyStatus         = new TreeMap<>();

    private static final Logger                        LOG                         = LoggerFactory
                                                                                       .getLogger(ReadOnlyServiceImpl.class);

    /**
     * 读取下标事件
     */
    private static class ReadIndexEvent {
        /**
         * 请求上下文
         */
        Bytes            requestContext;
        /**
         * 读取index 对应的 回调对象
         */
        ReadIndexClosure done;
        /**
         * 闭锁 用于实现同步等待结果
         */
        CountDownLatch   shutdownLatch;
        long             startTime;
    }

    /**
     * 对应 disruptor 事件工厂
     */
    private static class ReadIndexEventFactory implements EventFactory<ReadIndexEvent> {

        @Override
        public ReadIndexEvent newInstance() {
            return new ReadIndexEvent();
        }
    }

    /**
     * 处理线性一致性读任务
     */
    private class ReadIndexEventHandler implements EventHandler<ReadIndexEvent> {
        // task list for batch
        // 创建用来批处理任务的队列
        private final List<ReadIndexEvent> events = new ArrayList<>(
                                                      ReadOnlyServiceImpl.this.raftOptions.getApplyBatch());

        /**
         * 处理事件的逻辑
         * @param newEvent  读取index 的事件对象
         * @param sequence
         * @param endOfBatch
         * @throws Exception
         */
        @Override
        public void onEvent(final ReadIndexEvent newEvent, final long sequence, final boolean endOfBatch)
                                                                                                         throws Exception {
            // 如果该对象存在 闭锁  这时才执行任务
            if (newEvent.shutdownLatch != null) {
                executeReadIndexEvents(this.events);
                this.events.clear();
                newEvent.shutdownLatch.countDown();
                return;
            }

            // 将任务添加到 列表中
            this.events.add(newEvent);
            // 超过批量值 或者是最后一个任务 才执行
            if (this.events.size() >= ReadOnlyServiceImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
                executeReadIndexEvents(this.events);
                this.events.clear();
            }
        }
    }

    /**
     * ReadIndexResponse process closure
     *
     * @author dennis
     */
    class ReadIndexResponseClosure extends RpcResponseClosureAdapter<ReadIndexResponse> {

        /**
         * 每个state 对象都是 readIndexEvent 封装成的
         */
        final List<ReadIndexState> states;
        final ReadIndexRequest     request;

        public ReadIndexResponseClosure(final List<ReadIndexState> states, final ReadIndexRequest request) {
            super();
            this.states = states;
            this.request = request;
        }

        /**
         * Called when ReadIndex response returns.
         * 回调对象触发方法
         */
        @Override
        public void run(final Status status) {
            if (!status.isOk()) {
                notifyFail(status);
                return;
            }
            final ReadIndexResponse readIndexResponse = getResponse();
            if (!readIndexResponse.getSuccess()) {
                notifyFail(new Status(-1, "Fail to run ReadIndex task, maybe the leader stepped down."));
                return;
            }
            // Success
            final ReadIndexStatus readIndexStatus = new ReadIndexStatus(this.states, this.request,
                readIndexResponse.getIndex());
            // 更新每个state 的index
            for (final ReadIndexState state : this.states) {
                // Records current commit log index.
                state.setIndex(readIndexResponse.getIndex());
            }

            boolean doUnlock = true;
            ReadOnlyServiceImpl.this.lock.lock();
            try {
                // 代表获取到的偏移量对应的数据已经被发送给follower了
                if (readIndexStatus.isApplied(ReadOnlyServiceImpl.this.fsmCaller.getLastAppliedIndex())) {
                    // Already applied, notify readIndex request.
                    ReadOnlyServiceImpl.this.lock.unlock();
                    doUnlock = false;
                    notifySuccess(readIndexStatus);
                } else {
                    // Not applied, add it to pending-notify cache.
                    // 还没处理到这步 就先加入到闲置缓存中  利用treeMap 来保证顺序存储
                    ReadOnlyServiceImpl.this.pendingNotifyStatus
                        .computeIfAbsent(readIndexStatus.getIndex(), k -> new ArrayList<>(10)) //
                        .add(readIndexStatus);
                }
            } finally {
                if (doUnlock) {
                    ReadOnlyServiceImpl.this.lock.unlock();
                }
            }
        }

        /**
         * 当回调处理异常结果时触发
         * @param status
         */
        private void notifyFail(final Status status) {
            final long nowMs = Utils.monotonicMs();
            for (final ReadIndexState state : this.states) {
                ReadOnlyServiceImpl.this.nodeMetrics.recordLatency("read-index", nowMs - state.getStartTimeMs());
                final ReadIndexClosure done = state.getDone();
                if (done != null) {
                    // 传播到下面的 回调对象
                    final Bytes reqCtx = state.getRequestContext();
                    done.run(status, ReadIndexClosure.INVALID_LOG_INDEX, reqCtx != null ? reqCtx.get() : null);
                }
            }
        }
    }

    /**
     * 批量执行 readIndexEvents
     * @param events
     */
    private void executeReadIndexEvents(final List<ReadIndexEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        final ReadIndexRequest.Builder rb = ReadIndexRequest.newBuilder() //
            .setGroupId(this.node.getGroupId()) //
            .setServerId(this.node.getServerId().toString());

        final List<ReadIndexState> states = new ArrayList<>(events.size());

        for (final ReadIndexEvent event : events) {
            // 将每个请求上下文包装后 设置到 ReaderIndexRequest 中
            rb.addEntries(ZeroByteStringHelper.wrap(event.requestContext.get()));
            // 这里是批量发送 读请求 单个请求对象 也就是事件 被封装成一个 state
            states.add(new ReadIndexState(event.requestContext, event.done, event.startTime));
        }
        final ReadIndexRequest request = rb.build();

        // 使用node发送 获取readIndex 的请求
        this.node.handleReadIndexRequest(request, new ReadIndexResponseClosure(states, request));
    }

    /**
     * 初始化 只读 服务
     * @param opts
     * @return
     */
    @Override
    public boolean init(final ReadOnlyServiceOptions opts) {
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        this.fsmCaller = opts.getFsmCaller();
        this.raftOptions = opts.getRaftOptions();

        // 只读服务内部包含一个定时器
        this.scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("ReadOnlyService-PendingNotify-Scanner", true));
        // 启动 disruptor
        this.readIndexDisruptor = DisruptorBuilder.<ReadIndexEvent> newInstance() //
            .setEventFactory(new ReadIndexEventFactory()) //
            .setRingBufferSize(this.raftOptions.getDisruptorBufferSize()) //
            .setThreadFactory(new NamedThreadFactory("JRaft-ReadOnlyService-Disruptor-", true)) //
            .setWaitStrategy(new BlockingWaitStrategy()) //
            .setProducerType(ProducerType.MULTI) //
            .build();
        this.readIndexDisruptor.handleEventsWith(new ReadIndexEventHandler());
        this.readIndexDisruptor
            .setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName()));
        this.readIndexQueue = this.readIndexDisruptor.start();
        if(this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry() //
                .register("jraft-read-only-service-disruptor", new DisruptorMetricSet(this.readIndexQueue));
        }
        // listen on lastAppliedLogIndex change events.
        // 将自身添加到 caller 中 监听 lastIndex发生变化
        this.fsmCaller.addLastAppliedLogIndexListener(this);

        // start scanner
        this.scheduledExecutorService.scheduleAtFixedRate(
                // 定时获取 最新的提交数据的下标
            () -> onApplied(this.fsmCaller.getLastAppliedIndex()),
            this.raftOptions.getMaxElectionDelayMs(),
            this.raftOptions.getMaxElectionDelayMs(),
            TimeUnit.MILLISECONDS
        );
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        this.shutdownLatch = new CountDownLatch(1);
        this.readIndexQueue.publishEvent((event, sequence) -> event.shutdownLatch = this.shutdownLatch);
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
        }
        this.readIndexDisruptor.shutdown();
        this.scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * 增加一个请求对象
     * @param reqCtx    request context of readIndex
     * @param closure   callback
     */
    @Override
    public void addRequest(final byte[] reqCtx, final ReadIndexClosure closure) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(closure, new Status(RaftError.EHOSTDOWN, "Was stopped"));
            throw new IllegalStateException("Service already shutdown.");
        }
        try {
            EventTranslator<ReadIndexEvent> translator = (event, sequence) -> {
                event.done = closure;
                event.requestContext = new Bytes(reqCtx);
                event.startTime = Utils.monotonicMs();
            };
            int retryTimes = 0;
            while (true) {
                if (this.readIndexQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > MAX_ADD_REQUEST_RETRY_TIMES) {
                        Utils.runClosureInThread(closure,
                            new Status(RaftError.EBUSY, "Node is busy, has too many read-only requests."));
                        this.nodeMetrics.recordTimes("read-index-overload-times", 1);
                        LOG.warn("Node {} ReadOnlyServiceImpl readIndexQueue is overload.", this.node.getNodeId());
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }
        } catch (final Exception e) {
            Utils.runClosureInThread(closure, new Status(RaftError.EPERM, "Node is down."));
        }
    }

    /**
     * Called when lastAppliedIndex updates.
     * 传入 caller 最新提交的 index
     * @param appliedIndex applied index
     */
    @Override
    public void onApplied(final long appliedIndex) {
        // TODO reuse pendingStatuses list?
        List<ReadIndexStatus> pendingStatuses = null;
        this.lock.lock();
        try {
            // 处理未调用 notifySuccess 的state
            if (this.pendingNotifyStatus.isEmpty()) {
                return;
            }
            // Find all statuses that log index less than or equal to appliedIndex.
            final Map<Long, List<ReadIndexStatus>> statuses = this.pendingNotifyStatus.headMap(appliedIndex, true);
            if (statuses != null) {
                pendingStatuses = new ArrayList<>(statuses.size() << 1);

                final Iterator<Map.Entry<Long, List<ReadIndexStatus>>> it = statuses.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<Long, List<ReadIndexStatus>> entry = it.next();
                    pendingStatuses.addAll(entry.getValue());
                    // Remove the entry from statuses, it will also be removed in pendingNotifyStatus.
                    it.remove();
                }

            }
        } finally {
            this.lock.unlock();
            if (pendingStatuses != null && !pendingStatuses.isEmpty()) {
                for (final ReadIndexStatus status : pendingStatuses) {
                    notifySuccess(status);
                }
            }
        }
    }

    /**
     * Flush all events in disruptor.
     */
    @OnlyForTest
    void flush() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        this.readIndexQueue.publishEvent((task, sequence) -> task.shutdownLatch = latch);
        latch.await();
    }

    @OnlyForTest
    TreeMap<Long, List<ReadIndexStatus>> getPendingNotifyStatus() {
        return this.pendingNotifyStatus;
    }

    /**
     * 按成功方式 触发回调
     * @param status
     */
    private void notifySuccess(final ReadIndexStatus status) {
        final long nowMs = Utils.monotonicMs();
        final List<ReadIndexState> states = status.getStates();
        final int taskCount = states.size();
        for (int i = 0; i < taskCount; i++) {
            final ReadIndexState task = states.get(i);
            final ReadIndexClosure done = task.getDone(); // stack copy
            if (done != null) {
                this.nodeMetrics.recordLatency("read-index", nowMs - task.getStartTimeMs());
                done.setResult(task.getIndex(), task.getRequestContext().get());
                done.run(Status.OK());
            }
        }
    }
}
