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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReplicatorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.RecyclableByteBufferList;
import com.alipay.sofa.jraft.util.RecycleUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * Replicator for replicating log entry from leader to followers.
 * @author boyan (boyan@alibaba-inc.com)
 * 该对象负责将 数据从leader 复制到follower
 * 2018-Apr-04 10:32:02 AM
 */
@ThreadSafe
public class Replicator implements ThreadId.OnError {

    private static final Logger              LOG                    = LoggerFactory.getLogger(Replicator.class);

    /**
     * client 对象 (跟node中包含的是同一个对象) 用来跟follower 通信
     */
    private final RaftClientService          rpcService;
    // Next sending log index
    private volatile long                    nextIndex;
    /**
     * 最大允许的连续错误次数 看来一旦成功就会清零
     */
    private int                              consecutiveErrorTimes  = 0;
    private boolean                          hasSucceeded;
    private long                             timeoutNowIndex;
    /**
     * 代表最后一次发送的时间戳
     */
    private volatile long                    lastRpcSendTimestamp;
    /**
     * 心跳计数器
     */
    private volatile long                    heartbeatCounter       = 0;
    private volatile long                    appendEntriesCounter   = 0;
    private volatile long                    installSnapshotCounter = 0;
    protected Stat                           statInfo               = new Stat();
    private ScheduledFuture<?>               blockTimer;

    // Cached the latest RPC in-flight request. 该对象内部包含 最后一次发起的rpc Request 对象
    private Inflight                         rpcInFly;
    // Heartbeat RPC future  心跳消息 这里 Future 被看作是 飞行对象(也就是还未确认结果的对象)
    private Future<Message>                  heartbeatInFly;
    // Timeout request RPC future
    private Future<Message>                  timeoutNowInFly;
    // In-flight RPC requests, FIFO queue   存放飞行对象的队列
    private final ArrayDeque<Inflight>       inflights              = new ArrayDeque<>();

    private long                             waitId                 = -1L;
    /**
     * 对应该复制机的唯一标识 当进行复制时需要该参数
     */
    protected ThreadId                       id;
    private final ReplicatorOptions          options;
    private final RaftOptions                raftOptions;

    /**
     * 发送心跳的定时器
     */
    private ScheduledFuture<?>               heartbeatTimer;
    /**
     * 快照读取器  replicator 对象不仅有向 follower 发送logEntries 的能力 还有为其他节点安装快照的能力
     */
    private volatile SnapshotReader          reader;
    /**
     * 追赶回调  当其他节点变成leader时会使用该对象
     */
    private CatchUpClosure                   catchUpClosure;
    /**
     * 定时器管理器  实际上内部包含一个 ScheduleExecutorService
     */
    private final TimerManager               timerManager;
    private final NodeMetrics                nodeMetrics;
    /**
     * 状态信息
     */
    private volatile State                   state;

    // Request sequence  请求序列
    private int                              reqSeq                 = 0;
    // Response sequence  响应序列
    private int                              requiredNextSeq        = 0;
    // Replicator state reset version
    private int                              version                = 0;

    // Pending response queue;
    /**
     * 一个存放RPC 响应结果的 优先队列  RpcResponse 通过seq 来比较大小
     */
    private final PriorityQueue<RpcResponse> pendingResponses       = new PriorityQueue<>(50);

    /**
     * 增加 reqSeq 如果溢出 重置为0
     * @return
     */
    private int getAndIncrementReqSeq() {
        final int prev = this.reqSeq;
        this.reqSeq++;
        if (this.reqSeq < 0) {
            this.reqSeq = 0;
        }
        return prev;
    }

    /**
     * 增加 required 序列的
     * @return
     */
    private int getAndIncrementRequiredNextSeq() {
        final int prev = this.requiredNextSeq;
        this.requiredNextSeq++;
        if (this.requiredNextSeq < 0) {
            this.requiredNextSeq = 0;
        }
        return prev;
    }

    /**
     * Replicator state
     * @author dennis
     *
     */
    public enum State {
        Probe, // probe follower state   探测 follower 状态
        Snapshot, // installing snapshot to follower   为 follower 安装快照
        Replicate, // replicate logs normally    将LogEntry 复制到follower
        Destroyed // destroyed    销毁
    }

    /**
     * 初始化 复制机对象
     * @param replicatorOptions
     * @param raftOptions
     */
    public Replicator(final ReplicatorOptions replicatorOptions, final RaftOptions raftOptions) {
        super();
        this.options = replicatorOptions;
        this.nodeMetrics = this.options.getNode().getNodeMetrics();
        // 只有当成功将数据发送到 server 且从对端返回了数据 并正常处理后 nextIndex 才会增加 本次交互才算有效的
        this.nextIndex = this.options.getLogManager().getLastLogIndex() + 1;
        // 提供了一些快捷使用定时器的api
        this.timerManager = replicatorOptions.getTimerManager();
        this.raftOptions = raftOptions;
        // 应该是和node共用一个client 对象
        this.rpcService = replicatorOptions.getRaftRpcService();
    }

    /**
     * Replicator metric set.
     * @author dennis
     *
     */
    private static final class ReplicatorMetricSet implements MetricSet {
        private final ReplicatorOptions opts;
        private final Replicator        r;

        private ReplicatorMetricSet(final ReplicatorOptions opts, final Replicator r) {
            this.opts = opts;
            this.r = r;
        }

        @Override
        public Map<String, Metric> getMetrics() {
            final Map<String, Metric> gauges = new HashMap<>();
            gauges.put("log-lags",
                (Gauge<Long>) () -> this.opts.getLogManager().getLastLogIndex() - (this.r.nextIndex - 1));
            gauges.put("next-index", (Gauge<Long>) () -> this.r.nextIndex);
            gauges.put("heartbeat-times", (Gauge<Long>) () -> this.r.heartbeatCounter);
            gauges.put("install-snapshot-times", (Gauge<Long>) () -> this.r.installSnapshotCounter);
            gauges.put("append-entries-times", (Gauge<Long>) () -> this.r.appendEntriesCounter);
            return gauges;
        }
    }

    /**
     * Internal state
     * @author boyan (boyan@alibaba-inc.com)
     * 当前运行状态
     * 2018-Apr-04 10:38:45 AM
     */
    enum RunningState {
        IDLE, // idle   处于空闲状态
        BLOCKING, // blocking state   处于阻塞状态
        APPENDING_ENTRIES, // appending log entries  增加 LogEntry
        INSTALLING_SNAPSHOT // installing snapshot   安装快照
    }

    /**
     * 针对 stateListener 对应api 的事件
     */
    enum ReplicatorEvent {
        CREATED, // created  当replicator被创建时
        ERROR, // error      出现异常
        DESTROYED // destroyed  销毁时
    }

    /**
     * User can implement the ReplicatorStateListener interface by themselves.
     * So they can do some their own logic codes when replicator created, destroyed or had some errors.
     * 复制机状态监听器
     * @author zongtanghu
     *
     * 2019-Aug-20 2:32:10 PM
     */
    public interface ReplicatorStateListener {

        /**
         * Called when this replicator has been created.
         * 当某个 复制者被创建时 触发  peerId 对应replicator 的 节点id
         * @param peer   replicator related peerId
         */
        void onCreated(final PeerId peer);

        /**
         * Called when this replicator has some errors.
         * 当复制出现异常时 触发
         * @param peer   replicator related peerId
         * @param status replicator's error detailed status
         */
        void onError(final PeerId peer, final Status status);

        /**
         * Called when this replicator has been destroyed.
         * 销毁时触发
         * @param peer   replicator related peerId
         */
        void onDestroyed(final PeerId peer);
    }

    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users.
     * 根据事件类型转发到监听器的不同方法
     * @param replicator    replicator object
     * @param event         replicator's state listener event type
     * @param status        replicator's error detailed status
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event, final Status status) {
        final ReplicatorOptions replicatorOpts = Requires.requireNonNull(replicator.getOpts(), "replicatorOptions");
        final Node node = Requires.requireNonNull(replicatorOpts.getNode(), "node");
        final PeerId peer = Requires.requireNonNull(replicatorOpts.getPeerId(), "peer");

        // 获取所有监听器 根据事件触发对应回调
        final List<ReplicatorStateListener> listenerList = node.getReplicatorStatueListeners();
        for (int i = 0; i < listenerList.size(); i++) {
            final ReplicatorStateListener listener = listenerList.get(i);
            if(listener != null) {
                try {
                    switch (event) {
                        case CREATED:
                            Utils.runInThread(() -> listener.onCreated(peer));
                            break;
                        case ERROR:
                            Utils.runInThread(() -> listener.onError(peer, status));
                            break;
                        case DESTROYED:
                            Utils.runInThread(() -> listener.onDestroyed(peer));
                            break;
                        default:
                            break;
                    }
                } catch (final Exception e) {
                    LOG.error("Fail to notify ReplicatorStatusListener, listener={}, event={}.", listener, event);
                }
            }
        }
    }

    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users for none status.
     *
     * @param replicator    replicator object
     * @param event         replicator's state listener event type
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event) {
        notifyReplicatorStatusListener(replicator, event, null);
    }

    /**
     * Statistics structure
     * @author boyan (boyan@alibaba-inc.com)
     * 统计对象
     * 2018-Apr-04 10:38:53 AM
     */
    static class Stat {
        // 记录当前运行状态
        RunningState runningState;
        long         firstLogIndex;
        long         lastLogIncluded;
        long         lastLogIndex;
        long         lastTermIncluded;

        @Override
        public String toString() {
            return "<running=" + this.runningState + ", firstLogIndex=" + this.firstLogIndex + ", lastLogIncluded="
                   + this.lastLogIncluded + ", lastLogIndex=" + this.lastLogIndex + ", lastTermIncluded="
                   + this.lastTermIncluded + ">";
        }

    }

    // In-flight request type    异步请求的类型 1.为其他node安装快照  2.为其他节点发送LogEntries
    enum RequestType {
        Snapshot, // install snapshot
        AppendEntries // replicate logs
    }

    /**
     * In-flight request.
     * 就是对future 的包装  记录该组数据的偏移量  便于限流
     * @author dennis
     *
     */
    static class Inflight {
        // In-flight request count
        final int             count;
        // Start log index 代表该组数据的起始偏移量
        final long            startIndex;
        // Entries size in bytes  该组数据长度
        final int             size;
        // RPC future  用于获取结果
        final Future<Message> rpcFuture;
        /**
         * 请求类型 安装快照/增加LogEntry
         */
        final RequestType     requestType;
        // Request sequence.   当前请求对象序列
        final int             seq;

        public Inflight(final RequestType requestType, final long startIndex, final int count, final int size,
                        final int seq, final Future<Message> rpcFuture) {
            super();
            this.seq = seq;
            this.requestType = requestType;
            this.count = count;
            this.startIndex = startIndex;
            this.size = size;
            this.rpcFuture = rpcFuture;
        }

        @Override
        public String toString() {
            return "Inflight [count=" + this.count + ", startIndex=" + this.startIndex + ", size=" + this.size
                   + ", rpcFuture=" + this.rpcFuture + ", requestType=" + this.requestType + ", seq=" + this.seq + "]";
        }

        // 判断本次 future 对象是否是发送LogEntries  首先类型要匹配 其次count 必须大于0  因为 prob 也是使用了 AppendEntries 类型
        boolean isSendingLogEntries() {
            return this.requestType == RequestType.AppendEntries && this.count > 0;
        }
    }

    /**
     * RPC response for AppendEntries/InstallSnapshot.
     * 代表本次响应结果
     * @author dennis
     *
     */
    static class RpcResponse implements Comparable<RpcResponse> {
        /**
         * 响应结果状态
         */
        final Status      status;
        /**
         * 请求体也存放在里面
         */
        final Message     request;
        final Message     response;
        final long        rpcSendTime;
        /**
         * 响应序列
         */
        final int         seq;
        final RequestType requestType;

        public RpcResponse(final RequestType reqType, final int seq, final Status status, final Message request,
                           final Message response, final long rpcSendTime) {
            super();
            this.requestType = reqType;
            this.seq = seq;
            this.status = status;
            this.request = request;
            this.response = response;
            this.rpcSendTime = rpcSendTime;
        }

        @Override
        public String toString() {
            return "RpcResponse [status=" + this.status + ", request=" + this.request + ", response=" + this.response
                   + ", rpcSendTime=" + this.rpcSendTime + ", seq=" + this.seq + ", requestType=" + this.requestType
                   + "]";
        }

        /**
         * Sort by sequence.
         */
        @Override
        public int compareTo(final RpcResponse o) {
            return Integer.compare(this.seq, o.seq);
        }
    }

    @OnlyForTest
    ArrayDeque<Inflight> getInflights() {
        return this.inflights;
    }

    @OnlyForTest
    State getState() {
        return this.state;
    }

    @OnlyForTest
    void setState(final State state) {
        this.state = state;
    }

    @OnlyForTest
    int getReqSeq() {
        return this.reqSeq;
    }

    @OnlyForTest
    int getRequiredNextSeq() {
        return this.requiredNextSeq;
    }

    @OnlyForTest
    int getVersion() {
        return this.version;
    }

    @OnlyForTest
    public PriorityQueue<RpcResponse> getPendingResponses() {
        return this.pendingResponses;
    }

    @OnlyForTest
    long getWaitId() {
        return this.waitId;
    }

    @OnlyForTest
    ScheduledFuture<?> getBlockTimer() {
        return this.blockTimer;
    }

    @OnlyForTest
    long getTimeoutNowIndex() {
        return this.timeoutNowIndex;
    }

    @OnlyForTest
    ReplicatorOptions getOpts() {
        return this.options;
    }

    @OnlyForTest
    long getRealNextIndex() {
        return this.nextIndex;
    }

    @OnlyForTest
    Future<Message> getRpcInFly() {
        if (this.rpcInFly == null) {
            return null;
        }
        return this.rpcInFly.rpcFuture;
    }

    @OnlyForTest
    Future<Message> getHeartbeatInFly() {
        return this.heartbeatInFly;
    }

    @OnlyForTest
    ScheduledFuture<?> getHeartbeatTimer() {
        return this.heartbeatTimer;
    }

    @OnlyForTest
    void setHasSucceeded() {
        this.hasSucceeded = true;
    }

    @OnlyForTest
    Future<Message> getTimeoutNowInFly() {
        return this.timeoutNowInFly;
    }

    /**
     * Adds a in-flight request
     * 增加一个 飞行对象
     * @param reqType   type of request
     * @param count     count if request
     * @param size      size in bytes
     */
    private void addInflight(final RequestType reqType, final long startIndex, final int count, final int size,
                             final int seq, final Future<Message> rpcInfly) {
        this.rpcInFly = new Inflight(reqType, startIndex, count, size, seq, rpcInfly);
        // 往队列中添加 inflight
        this.inflights.add(this.rpcInFly);
        this.nodeMetrics.recordSize("replicate-inflights-count", this.inflights.size());
    }

    /**
     * Returns the next in-flight sending index, return -1 when can't send more in-flight requests.
     * 获取下一个待发送的下标
     * @return next in-flight sending index
     */
    long getNextSendIndex() {
        // Fast path
        if (this.inflights.isEmpty()) {
            // nextIndex 应该是一个全局递增的标识  从这里看出 nextIndex 记录的是不包含 在inflights 数据的偏移量
            return this.nextIndex;
        }
        // Too many in-flight requests.  超过了最多允许等待的请求数
        if (this.inflights.size() > this.raftOptions.getMaxReplicatorInflightMsgs()) {
            return -1L;
        }
        // Last request should be a AppendEntries request and has some entries.
        // 待发送的数据长读要计算在内
        if (this.rpcInFly != null && this.rpcInFly.isSendingLogEntries()) {
            return this.rpcInFly.startIndex + this.rpcInFly.count;
        }
        return -1L;
    }

    /**
     * 从待处理future 中拉取对象
     * @return
     */
    private Inflight pollInflight() {
        return this.inflights.poll();
    }

    /**
     * 开启单次延时任务 在指定延时后 如果id 不为空 就设置超时异常
     * @param startMs
     */
    private void startHeartbeatTimer(final long startMs) {
        final long dueTime = startMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            this.heartbeatTimer = this.timerManager.schedule(() -> onTimeout(this.id), dueTime - Utils.nowMs(),
                TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOG.error("Fail to schedule heartbeat timer", e);
            onTimeout(this.id);
        }
    }

    /**
     * 安装快照
     */
    void installSnapshot() {
        // 如果当前正在安装快照就 忽略新的安装请求
        if (this.state == State.Snapshot) {
            LOG.warn("Replicator {} is installing snapshot, ignore the new request.", this.options.getPeerId());
            this.id.unlock();
            return;
        }
        boolean doUnlock = true;
        try {
            Requires.requireTrue(this.reader == null,
                "Replicator %s already has a snapshot reader, current state is %s", this.options.getPeerId(),
                this.state);
            // 开启本地快照读取对象从 这里先粗略的理解为reader 连接到了某个文件
            this.reader = this.options.getSnapshotStorage().open();
            if (this.reader == null) {
                // 如果快照对象还没有创建 这里要抛出异常
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to open snapshot"));
                this.id.unlock();
                doUnlock = false;
                node.onError(error);
                return;
            }
            // 创建一个用于复制的 url
            final String uri = this.reader.generateURIForCopy();
            // 没有生成 url抛出异常
            if (uri == null) {
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to generate uri for snapshot reader"));
                releaseReader();
                this.id.unlock();
                doUnlock = false;
                node.onError(error);
                return;
            }
            // 加载快照数据
            final RaftOutter.SnapshotMeta meta = this.reader.load();
            // 不存在快照数据抛出异常
            if (meta == null) {
                final String snapshotPath = this.reader.getPath();
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to load meta from %s", snapshotPath));
                releaseReader();
                this.id.unlock();
                doUnlock = false;
                node.onError(error);
                return;
            }
            final InstallSnapshotRequest.Builder rb = InstallSnapshotRequest.newBuilder();
            rb.setTerm(this.options.getTerm());
            rb.setGroupId(this.options.getGroupId());
            rb.setServerId(this.options.getServerId().toString());
            rb.setPeerId(this.options.getPeerId().toString());
            rb.setMeta(meta);
            rb.setUri(uri);

            this.statInfo.runningState = RunningState.INSTALLING_SNAPSHOT;
            this.statInfo.lastLogIncluded = meta.getLastIncludedIndex();
            this.statInfo.lastTermIncluded = meta.getLastIncludedTerm();

            final InstallSnapshotRequest request = rb.build();
            this.state = State.Snapshot;
            // noinspection NonAtomicOperationOnVolatileField
            this.installSnapshotCounter++;
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final int stateVersion = this.version;
            final int seq = getAndIncrementReqSeq();
            // 创建安装快照的请求 并发送
            final Future<Message> rpcFuture = this.rpcService.installSnapshot(this.options.getPeerId().getEndpoint(),
                request, new RpcResponseClosureAdapter<InstallSnapshotResponse>() {

                    @Override
                    public void run(final Status status) {
                        onRpcReturned(Replicator.this.id, RequestType.Snapshot, status, request, getResponse(), seq,
                            stateVersion, monotonicSendTimeMs);
                    }
                });
            // 增加一个 等待响应的飞行对象
            addInflight(RequestType.Snapshot, this.nextIndex, 0, 0, seq, rpcFuture);
        } finally {
            if (doUnlock) {
                this.id.unlock();
            }
        }
    }

    /**
     * 当安装快照返回后
     * @param id
     * @param r
     * @param status
     * @param request
     * @param response
     * @return
     */
    @SuppressWarnings("unused")
    static boolean onInstallSnapshotReturned(final ThreadId id, final Replicator r, final Status status,
                                             final InstallSnapshotRequest request,
                                             final InstallSnapshotResponse response) {
        boolean success = true;
        // 将reader 引用置空
        r.releaseReader();
        // noinspection ConstantConditions
        do {
            final StringBuilder sb = new StringBuilder("Node "). //
                append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                append(" received InstallSnapshotResponse from ").append(r.options.getPeerId()). //
                append(" lastIncludedIndex=").append(request.getMeta().getLastIncludedIndex()). //
                append(" lastIncludedTerm=").append(request.getMeta().getLastIncludedTerm());
            if (!status.isOk()) {
                sb.append(" error:").append(status);
                LOG.info(sb.toString());
                // 触发异常回调  由用户实现
                notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
                if (++r.consecutiveErrorTimes % 10 == 0) {
                    LOG.warn("Fail to install snapshot at peer={}, error={}", r.options.getPeerId(), status);
                }
                success = false;
                break;
            }
            if (!response.getSuccess()) {
                sb.append(" success=false");
                LOG.info(sb.toString());
                success = false;
                break;
            }
            // success  更新nextIndex
            // 代表本次请求成功 更新下次发送数据的起点 等下要发送新的数据时 index 是怎么确定的???
            r.nextIndex = request.getMeta().getLastIncludedIndex() + 1;
            sb.append(" success=true");
            LOG.info(sb.toString());
        } while (false);
        // We don't retry installing the snapshot explicitly.
        // id is unlock in sendEntries
        if (!success) {
            //should reset states
            // 如果本次请求失败 比如发送 1，2，3，4，5 的数据 如果2发送失败了 3，4，5 都要被丢弃  也就是清空 inflights
            r.resetInflights();
            r.state = State.Probe;
            // 通过定时器发送一次探测请求
            r.block(Utils.nowMs(), status.getCode());
            return false;
        }
        r.hasSucceeded = true;
        // 当成功接受到快照时 触发一个追赶回调
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        // 这是做什么的???
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        // id is unlock in _send_entriesheartbeatCounter
        r.state = State.Replicate;
        return true;
    }

    /**
     * 发送空的实体 根据是否是心跳 发送数据
     * 当复制机被初始化 调用start 时 会调用该方法 此时的heartbeat == false 代表发送的是探测请求
     * @param isHeartbeat
     */
    private void sendEmptyEntries(final boolean isHeartbeat) {
        sendEmptyEntries(isHeartbeat, null);
    }

    /**
     * Send probe or heartbeat request
     * 发送探测请求 或者心跳请求
     * @param isHeartbeat      if current entries is heartbeat
     * @param heartBeatClosure heartbeat callback   回调可以为null
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void sendEmptyEntries(final boolean isHeartbeat,
                                  final RpcResponseClosure<AppendEntriesResponse> heartBeatClosure) {
        // 构建一组 追加 Entries 的请求 对应到leader往其他follower 发送数据
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        // 填充公共字段  TODO 这里要注意 内部有一个 index 不为0 任期为0的情况 这里的考虑是安装快照
        if (!fillCommonFields(rb, this.nextIndex - 1, isHeartbeat)) {
            // id is unlock in installSnapshot
            installSnapshot();
            if (isHeartbeat && heartBeatClosure != null) {
                Utils.runClosureInThread(heartBeatClosure, new Status(RaftError.EAGAIN,
                    "Fail to send heartbeat to peer %s", this.options.getPeerId()));
            }
            return;
        }
        try {
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final AppendEntriesRequest request = rb.build();

            // 如果是心跳请求
            // 这里的心跳请求 就映射到 leader 要定期向所有follower 发送心跳 避免其他节点开始选举
            // 同时也会收到follower 的term 如果发现自己落后了 就要考虑是否要将自己的角色变更
            if (isHeartbeat) {
                // Sending a heartbeat request
                this.heartbeatCounter++;
                RpcResponseClosure<AppendEntriesResponse> heartbeatDone;
                // Prefer passed-in closure.
                if (heartBeatClosure != null) {
                    heartbeatDone = heartBeatClosure;
                } else {
                    // 如果没有设置回调的情况下 这里会使用默认的回调
                    heartbeatDone = new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            onHeartbeatReturned(Replicator.this.id, status, request, getResponse(), monotonicSendTimeMs);
                        }
                    };
                }
                // 发送心跳对象  这里超时时间 为 重新选举的一半 避免发送该数据耗时过长 甚至重新发起了选举
                // 而心跳包也就是 数据中不包含 data 和 entries_
                this.heartbeatInFly = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(), request,
                    this.options.getElectionTimeoutMs() / 2, heartbeatDone);
            // 如果是探测请求
            } else {
                // Sending a probe request.
                this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
                this.statInfo.firstLogIndex = this.nextIndex;
                this.statInfo.lastLogIndex = this.nextIndex - 1;
                this.appendEntriesCounter++;
                // 代表正在探测中
                this.state = State.Probe;
                final int stateVersion = this.version;
                // 累加请求次数
                final int seq = getAndIncrementReqSeq();
                final Future<Message> rpcFuture = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(),
                    request, -1, new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            onRpcReturned(Replicator.this.id, RequestType.AppendEntries, status, request,
                                getResponse(), seq, stateVersion, monotonicSendTimeMs);
                        }

                    });

                // 添加一个待处理的future对象
                addInflight(RequestType.AppendEntries, this.nextIndex, 0, 0, seq, rpcFuture);
            }
            LOG.debug("Node {} send HeartbeatRequest to {} term {} lastCommittedIndex {}", this.options.getNode()
                .getNodeId(), this.options.getPeerId(), this.options.getTerm(), request.getCommittedIndex());
        } finally {
            this.id.unlock();
        }
    }

    /**
     * 将其他属性填充到 entry 中
     * @param nextSendingIndex
     * @param offset  offset在 0 ～ maxEntry 之间
     * @param emb  如果 LogEntry 是 conf 类型 将 2组peerIdList 设置到 EntryMeta
     * @param dateBuffer  如果LogEntry 是 data类型 就加入到buffer中
     * @return
     */
    boolean prepareEntry(final long nextSendingIndex, final int offset, final RaftOutter.EntryMeta.Builder emb,
                         final RecyclableByteBufferList dateBuffer) {
        // 每当往 databuffer 中插入数据 capacity 都会增加 所以当超过 max 时 就代表填充满了
        if (dateBuffer.getCapacity() >= this.raftOptions.getMaxBodySize()) {
            return false;
        }
        // 代表下个要选择的 偏移量
        final long logIndex = nextSendingIndex + offset;
        // 从存储数据的 logManager 中获取数据  如果用户是一批一批的复制数据 那么应该在前面的数据成功发送到半数后才进行下次的发送吧
        // 这里应该要通过什么标识来确保nextSendingIndex 不要将2次任务数据混起来
        final LogEntry entry = this.options.getLogManager().getEntry(logIndex);
        if (entry == null) {
            return false;
        }
        // 将存储在 logManager中的任期和类型转移到 emb 中 这时数据还不一定会写入 那么是从那么内存中的数据中获取吗
        emb.setTerm(entry.getId().getTerm());
        if (entry.hasChecksum()) {
            emb.setChecksum(entry.getChecksum()); //since 1.2.6
        }
        emb.setType(entry.getType());
        // 代表本次entry 是 conf
        if (entry.getPeers() != null) {
            Requires.requireTrue(!entry.getPeers().isEmpty(), "Empty peers at logIndex=%d", logIndex);
            for (final PeerId peer : entry.getPeers()) {
                emb.addPeers(peer.toString());
            }
            if (entry.getOldPeers() != null) {
                for (final PeerId peer : entry.getOldPeers()) {
                    emb.addOldPeers(peer.toString());
                }
            }
        } else {
            // 确保必须是 数据类型
            Requires.requireTrue(entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION,
                "Empty peers but is ENTRY_TYPE_CONFIGURATION type at logIndex=%d", logIndex);
        }
        final int remaining = entry.getData() != null ? entry.getData().remaining() : 0;
        emb.setDataLen(remaining);
        // 如果是data 类型 会写入到 databuffer 中
        if (entry.getData() != null) {
            // should slice entry data  切片对象的特性就是 共用一个 byte[] 但是拥有独立的指针
            dateBuffer.add(entry.getData().slice());
        }
        return true;
    }

    /**
     * 通过配置对象初始化 ThreadId  复制机的复制功能依赖于 threadId
     * 每个 node 包含一个 replicatorGroup 对象 而该对象内包含了除自身之外所有的peer 所对应的 replicator
     * 也就是实际上一次有多个 replicator 在监听logManager 的指针移动  当指针变化后 每个replicator 都会将请求发送到对应的peer 上
     * @param opts
     * @param raftOptions
     * @return
     */
    public static ThreadId start(final ReplicatorOptions opts, final RaftOptions raftOptions) {
        if (opts.getLogManager() == null || opts.getBallotBox() == null || opts.getNode() == null) {
            throw new IllegalArgumentException("Invalid ReplicatorOptions.");
        }
        final Replicator r = new Replicator(opts, raftOptions);
        // 每个follower 对象都会被该 client 连接  里头的逻辑跟seats 很像 就是获取从连接池中获取一条连接 获取不到就初始化一个 这时就触发了 Netty.Bootstrap.connect()
        if (!r.rpcService.connect(opts.getPeerId().getEndpoint())) {
            LOG.error("Fail to init sending channel to {}.", opts.getPeerId());
            // Return and it will be retried later.
            return null;
        }

        // Register replicator metric set.  统计相关的不看
        final MetricRegistry metricRegistry = opts.getNode().getNodeMetrics().getMetricRegistry();
        if (metricRegistry != null) {
            try {
                final String replicatorMetricName = getReplicatorMetricName(opts);
                if (!metricRegistry.getNames().contains(replicatorMetricName)) {
                    metricRegistry.register(replicatorMetricName, new ReplicatorMetricSet(opts, r));
                }
            } catch (final IllegalArgumentException e) {
                // ignore
            }
        }

        // Start replication
        r.id = new ThreadId(r, r);
        r.id.lock();
        // 触发创建钩子  jraft 没有默认实现 用户自行拓展
        notifyReplicatorStatusListener(r, ReplicatorEvent.CREATED);
        LOG.info("Replicator={}@{} is started", r.id, r.options.getPeerId());
        // 当初始化 复制机 时 追赶相关的回调要置空
        r.catchUpClosure = null;
        r.lastRpcSendTimestamp = Utils.monotonicMs();
        // 该定时任务会在 一定延时后触发 通过 onError 调用        r.sendEmptyEntries(true);  算是一种另类的心跳实现了
        r.startHeartbeatTimer(Utils.nowMs());
        // id.unlock in sendEmptyEntries
        // 本对象刚启动时 需要发送一个 无 conf 无 data 的空数据去检测对端follower 的状态(对端会把它的任期等信息返回回来 之后根据该情况判断是否要做降级)
        r.sendEmptyEntries(false);
        return r.id;
    }

    private static String getReplicatorMetricName(final ReplicatorOptions opts) {
        return "replicator-" + opts.getNode().getGroupId() + "/" + opts.getPeerId();
    }

    /**
     * 设置追赶用的回调   必须确保当前 catchUpClosure 为null
     * @param id
     * @param maxMargin
     * @param dueTime
     * @param done
     */
    public static void waitForCaughtUp(final ThreadId id, final long maxMargin, final long dueTime,
                                       final CatchUpClosure done) {
        final Replicator r = (Replicator) id.lock();

        if (r == null) {
            Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "No such replicator"));
            return;
        }
        try {
            if (r.catchUpClosure != null) {
                LOG.error("Previous wait_for_caught_up is not over");
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Duplicated call"));
                return;
            }
            done.setMaxMargin(maxMargin);
            if (dueTime > 0) {
                // 设置了某个定时任务  每隔多少时间 触发一次
                done.setTimer(r.timerManager.schedule(() -> onCatchUpTimedOut(id), dueTime - Utils.nowMs(),
                    TimeUnit.MILLISECONDS));
            }
            r.catchUpClosure = done;
        } finally {
            id.unlock();
        }
    }

    @Override
    public String toString() {
        return "Replicator [state=" + this.state + ", statInfo=" + this.statInfo + ",peerId="
               + this.options.getPeerId() + "]";
    }

    static void onBlockTimeoutInNewThread(final ThreadId id) {
        if (id != null) {
            continueSending(id, RaftError.ETIMEDOUT.getNumber());
        }
    }

    /**
     * Unblock and continue sending right now.
     */
    static void unBlockAndSendNow(final ThreadId id) {
        if (id == null) {
            // It was destroyed already
            return;
        }
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        try {
            if (r.blockTimer != null) {
                if (r.blockTimer.cancel(true)) {
                    onBlockTimeout(id);
                }
            }
        } finally {
            id.unlock();
        }
    }

    /**
     * 将数据传播到其他节点
     * @param id 对应本复制机的id
     * @param errCode  异常信息 非0代表出现异常 当LogManager已经被关闭时会返回该标识
     * @return
     */
    static boolean continueSending(final ThreadId id, final int errCode) {
        if (id == null) {
            //It was destroyed already
            // 当复制机被关闭时 id 会被销毁
            return true;
        }
        // 使用ThreadId上锁
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        r.waitId = -1;
        // 确定是超时异常
        if (errCode == RaftError.ETIMEDOUT.getNumber()) {
            // Send empty entries after block timeout to check the correct
            // _next_index otherwise the replicator is likely waits in            executor.shutdown();
            // _wait_more_entries and no further logs would be replicated even if the
            // last_index of this followers is less than |next_index - 1|
            // 发送一个探测对象
            r.sendEmptyEntries(false);
        } else if (errCode != RaftError.ESTOP.getNumber()) {
            // id is unlock in _send_entries 正常情况 开始发送数据
            r.sendEntries();
        } else {
            // 代表 LogManager 已经关闭 就会停止发送数据
            LOG.warn("Replicator {} stops sending entries.", id);
            id.unlock();
        }
        return true;
    }

    static void onBlockTimeout(final ThreadId arg) {
        Utils.runInThread(() -> onBlockTimeoutInNewThread(arg));
    }

    /**
     * 阻塞一定时间后发送一个探测请求
     * @param startTimeMs
     * @param errorCode
     */
    void block(final long startTimeMs, @SuppressWarnings("unused") final int errorCode) {
        // TODO: Currently we don't care about error_code which indicates why the
        // very RPC fails. To make it better there should be different timeout for
        // each individual error (e.g. we don't need check every
        // heartbeat_timeout_ms whether a dead follower has come back), but it's just
        // fine now.
        final long dueTime = startTimeMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            LOG.debug("Blocking {} for {} ms", this.options.getPeerId(), this.options.getDynamicHeartBeatTimeoutMs());
            this.blockTimer = this.timerManager.schedule(() -> onBlockTimeout(this.id), dueTime - Utils.nowMs(),
                TimeUnit.MILLISECONDS);
            this.statInfo.runningState = RunningState.BLOCKING;
            this.id.unlock();
        } catch (final Exception e) {
            LOG.error("Fail to add timer", e);
            // id unlock in sendEmptyEntries.
            sendEmptyEntries(false);
        }
    }

    /**
     * 当出现异常时触发
     * @param id        the thread id
     * @param data      the data
     * @param errorCode the error code
     */
    @Override
    public void onError(final ThreadId id, final Object data, final int errorCode) {
        final Replicator r = (Replicator) data;

        // 延时心跳产生的异常是 ETIMEDOUT
        // 当终止复制机的时候 会触发 ESTOP
        if (errorCode == RaftError.ESTOP.getNumber()) {
            try {
                for (final Inflight inflight : r.inflights) {
                    if (inflight != r.rpcInFly) {
                        // 唤醒所有阻塞线程  这个future 有哪些线程在阻塞获取结果吗???
                        inflight.rpcFuture.cancel(true);
                    }
                }
                // 记录当前的 fly 对象 也要关闭
                if (r.rpcInFly != null) {
                    r.rpcInFly.rpcFuture.cancel(true);
                    r.rpcInFly = null;
                }
                if (r.heartbeatInFly != null) {
                    r.heartbeatInFly.cancel(true);
                    r.heartbeatInFly = null;
                }
                if (r.timeoutNowInFly != null) {
                    r.timeoutNowInFly.cancel(true);
                    r.timeoutNowInFly = null;
                }
                if (r.heartbeatTimer != null) {
                    r.heartbeatTimer.cancel(true);
                    r.heartbeatTimer = null;
                }
                if (r.blockTimer != null) {
                    r.blockTimer.cancel(true);
                    r.blockTimer = null;
                }
                if (r.waitId >= 0) {
                    r.options.getLogManager().removeWaiter(r.waitId);
                }
                // 通知追赶  这里 beforeDestroy = true 其他方法都是false
                r.notifyOnCaughtUp(errorCode, true);
            } finally {
                // 触发销毁方法
                r.destroy();
            }
            // 代表心跳超时  通过这种方式来发送心跳???  每当调用心跳方法后 只要 id 还有效 也就是不为null 那么下次就会继续触发onError
            // 然后触发 sendHeartbeat  继续发送心跳
        } else if (errorCode == RaftError.ETIMEDOUT.getNumber()) {
            id.unlock();
            Utils.runInThread(() -> sendHeartbeat(id));
        } else {
            id.unlock();
            // noinspection ConstantConditions
            Requires.requireTrue(false, "Unknown error code for replicator: " + errorCode);
        }
    }

    /**
     * 每隔指定的时间 触发一次 onCaughtUp
     * @param id
     */
    private static void onCatchUpTimedOut(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        try {
            r.notifyOnCaughtUp(RaftError.ETIMEDOUT.getNumber(), false);
        } finally {
            id.unlock();
        }
    }

    /**
     * 当发现本节点已经落后于其他节点时触发
     * @param code
     * @param beforeDestroy  代表是否要先销毁数据
     */
    private void notifyOnCaughtUp(final int code, final boolean beforeDestroy) {
        // 如果没有设置 追赶用的回调对象 无视该方法  该回调最后会通向 node.onCaughtUp()
        if (this.catchUpClosure == null) {
            return;
        }
        // 代表 该leader 不再是leader 了 可能产生了新的 leader
        if (code != RaftError.ETIMEDOUT.getNumber()) {
            if (this.nextIndex - 1 + this.catchUpClosure.getMaxMargin() < this.options.getLogManager()
                .getLastLogIndex()) {
                return;
            }
            // 如果该回调已经设置了 异常就不再触发了
            if (this.catchUpClosure.isErrorWasSet()) {
                return;
            }
            // 这里是基于异常情况触发的 所以设置 errorWasSet = true
            this.catchUpClosure.setErrorWasSet(true);
            // 如果不是成功 就将异常设置到 回调中
            if (code != RaftError.SUCCESS.getNumber()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
            // 如果存在定时器  那么就不需要在这里触发了
            if (this.catchUpClosure.hasTimer()) {
                if (!beforeDestroy && !this.catchUpClosure.getTimer().cancel(true)) {
                    // There's running timer task, let timer task trigger
                    // on_caught_up to void ABA problem
                    return;
                }
            }
        } else {
            // timed out
            // 如果是触发了 超时异常
            if (!this.catchUpClosure.isErrorWasSet()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
        }
        final CatchUpClosure savedClosure = this.catchUpClosure;
        this.catchUpClosure = null;
        // 在其他线程中触发回调 同时置空 本 catchUpClosure
        Utils.runClosureInThread(savedClosure, savedClosure.getStatus());
    }

    private static void onTimeout(final ThreadId id) {
        if (id != null) {
            id.setError(RaftError.ETIMEDOUT.getNumber());
        } else {
            LOG.warn("Replicator id is null when timeout, maybe it's destroyed.");
        }
    }

    void destroy() {
        final ThreadId savedId = this.id;
        LOG.info("Replicator {} is going to quit", savedId);
        this.id = null;
        releaseReader();
        // Unregister replicator metric set
        if (this.options.getNode().getNodeMetrics().isEnabled()) {
            this.options.getNode().getNodeMetrics().getMetricRegistry().remove(getReplicatorMetricName(this.options));
        }
        this.state = State.Destroyed;
        notifyReplicatorStatusListener((Replicator) savedId.getData(), ReplicatorEvent.DESTROYED);
        savedId.unlockAndDestroy();
    }

    /**
     * 将 reader 引用置空
     */
    private void releaseReader() {
        if (this.reader != null) {
            Utils.closeQuietly(this.reader);
            this.reader = null;
        }
    }

    /**
     * 当心跳请求返回结果时
     * @param id
     * @param status
     * @param request
     * @param response
     * @param rpcSendTime
     */
    static void onHeartbeatReturned(final ThreadId id, final Status status, final AppendEntriesRequest request,
                                    final AppendEntriesResponse response, final long rpcSendTime) {

        // 代表该复制机已经无效了
        if (id == null) {
            // replicator already was destroyed.
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }
        boolean doUnlock = true;
        try {
            final boolean isLogDebugEnabled = LOG.isDebugEnabled();
            StringBuilder sb = null;
            if (isLogDebugEnabled) {
                sb = new StringBuilder("Node "). //
                    append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                    append(" received HeartbeatResponse from "). //
                    append(r.options.getPeerId()). //
                    append(" prevLogIndex=").append(request.getPrevLogIndex()). //
                    append(" prevLogTerm=").append(request.getPrevLogTerm());
            }
            if (!status.isOk()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, sleep.");
                    LOG.debug(sb.toString());
                }
                r.state = State.Probe;
                // 触发对应钩子 同样没有 默认实现
                notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
                // 增加连续的失败次数
                if (++r.consecutiveErrorTimes % 10 == 0) {
                    LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}", r.options.getPeerId(),
                        r.consecutiveErrorTimes, status);
                }
                // 在指定的时间后 id如果不为null 触发 onError 方法
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            // 代表本次心跳成功 清除失败次数
            r.consecutiveErrorTimes = 0;
            // 如果本节点发现自己已经落后了 销毁本对象
            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ").append(response.getTerm()).append(" expect term ")
                        .append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                final NodeImpl node = r.options.getNode();
                // 通知追上新的任期节点
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                // 销毁本复制机 因为本节点已经不再是 leader 了 也就不需要向其他follower 发送数据
                r.destroy();
                // 增加本节点任期
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Leader receives higher term heartbeat_response from peer:%s", r.options.getPeerId()));
                return;
            }
            // 本次请求失败
            if (!response.getSuccess() && response.hasLastLogIndex()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, response term ").append(response.getTerm()).append(" lastLogIndex ")
                        .append(response.getLastLogIndex());
                    LOG.debug(sb.toString());
                }
                LOG.warn("Heartbeat to peer {} failure, try to send a probe request.", r.options.getPeerId());
                doUnlock = false;
                // 发送一次探测请求
                r.sendEmptyEntries(false);
                // 启动一次心跳任务
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            if (isLogDebugEnabled) {
                LOG.debug(sb.toString());
            }
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            // 成功情况下也要发送心跳  该方法会在一定间隔后判断本id 是否== null 不为null 会触发 onError
            r.startHeartbeatTimer(startTimeMs);
        } finally {
            if (doUnlock) {
                id.unlock();
            }
        }
    }

    /**
     * 当往某个follower 发送数据 成功后触发
     * @param id
     * @param reqType  代表本次请求的类型
     * @param status   本次结果
     * @param request
     * @param response   server 设置的结果
     * @param seq
     * @param stateVersion   状态机的版本可能在某次修改 整个集群信息后该值就会发生变化
     * @param rpcSendTime
     */
    @SuppressWarnings("ContinueOrBreakFromFinallyBlock")
    static void onRpcReturned(final ThreadId id, final RequestType reqType, final Status status, final Message request,
                              final Message response, final int seq, final int stateVersion, final long rpcSendTime) {
        if (id == null) {
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        // id 内部包含复制机对象
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }

        // 代表响应结果回来时 复制机已经更新过一次了 不再处理旧数据  version 会在resetInFlight时触发
        // 比如发出 1，2，3，4，5 然后2 失败了 3，4，5 就会在inflight中被清除 然后可能还是会收到3，4，5 的res
        // 这时发现与当前的版本已经不匹配了 就忽略处理了
        if (stateVersion != r.version) {
            LOG.debug(
                "Replicator {} ignored old version response {}, current version is {}, request is {}\n, and response is {}\n, status is {}.",
                r, stateVersion, r.version, request, response, status);
            id.unlock();
            return;
        }

        // 将本次调用的结果包装成res 对象后添加到优先队列中
        final PriorityQueue<RpcResponse> holdingQueue = r.pendingResponses;
        holdingQueue.add(new RpcResponse(reqType, seq, status, request, response, rpcSendTime));

        // 如果超过了最大闲置数量
        if (holdingQueue.size() > r.raftOptions.getMaxReplicatorInflightMsgs()) {
            LOG.warn("Too many pending responses {} for replicator {}, maxReplicatorInflightMsgs={}",
                holdingQueue.size(), r.options.getPeerId(), r.raftOptions.getMaxReplicatorInflightMsgs());
            // 同时还清空了 holdingQueue 和 reader
            r.resetInflights();
            r.state = State.Probe;
            // TODO 发送一次探测请求  在收到结果后又会进入该方法  也就是相当于绕了一个圈 在没有收到正常的结果时需要一种方式来自动发送
            // 请求
            r.sendEmptyEntries(false);
            return;
        }

        boolean continueSendEntries = false;

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Replicator ").append(r).append(" is processing RPC responses,");
        }
        try {
            int processed = 0;
            // 开始处理 队列中囤积的响应结果 同样的 client 也就是leader 也必须按照顺序来处理响应结果 这样在投票箱
            // 那里就能解决乱序的问题
            while (!holdingQueue.isEmpty()) {
                final RpcResponse queuedPipelinedResponse = holdingQueue.peek();

                //sequence mismatch, waiting for next response.
                // 代表非预期的顺序  等待下次收到响应结果
                if (queuedPipelinedResponse.seq != r.requiredNextSeq) {
                    if (processed > 0) {
                        if (isLogDebugEnabled) {
                            sb.append("has processed ").append(processed).append(" responses,");
                        }
                        break;
                    } else {
                        //Do not processed any responses, UNLOCK id and return.
                        continueSendEntries = false;
                        id.unlock();
                        return;
                    }
                }
                // 代表正常处理 响应结果
                holdingQueue.remove();
                processed++;
                // 这里同时获取到 飞行对象
                final Inflight inflight = r.pollInflight();
                if (inflight == null) {
                    // The previous in-flight requests were cleared.
                    if (isLogDebugEnabled) {
                        sb.append("ignore response because request not found:").append(queuedPipelinedResponse)
                            .append(",\n");
                    }
                    continue;
                }
                // req 肯定是顺序发送的 也就是inflight 肯定是顺序发送 而响应结果 如果通过优先队列排序后
                // 且与预期值一致 那么这里就应该相等
                if (inflight.seq != queuedPipelinedResponse.seq) {
                    // reset state
                    LOG.warn(
                        "Replicator {} response sequence out of order, expect {}, but it is {}, reset state to try again.",
                        r, inflight.seq, queuedPipelinedResponse.seq);
                    // 代表出现异常所以清除所有飞行对象
                    r.resetInflights();
                    r.state = State.Probe;
                    continueSendEntries = false;
                    // 在一定延时后 发送一个探测请求
                    r.block(Utils.nowMs(), RaftError.EREQUEST.getNumber());
                    return;
                }
                try {
                    // 根据请求类型走不同处理
                    switch (queuedPipelinedResponse.requestType) {
                        case AppendEntries:
                            continueSendEntries = onAppendEntriesReturned(id, inflight, queuedPipelinedResponse.status,
                                (AppendEntriesRequest) queuedPipelinedResponse.request,
                                (AppendEntriesResponse) queuedPipelinedResponse.response, rpcSendTime, startTimeMs, r);
                            break;
                        case Snapshot:
                            continueSendEntries = onInstallSnapshotReturned(id, r, queuedPipelinedResponse.status,
                                (InstallSnapshotRequest) queuedPipelinedResponse.request,
                                (InstallSnapshotResponse) queuedPipelinedResponse.response);
                            break;
                    }
                    // 返回的结果决定了是否要继续发送数据
                } finally {
                    if (continueSendEntries) {
                        // Success, increase the response sequence.
                        r.getAndIncrementRequiredNextSeq();
                    } else {
                        // The id is already unlocked in onAppendEntriesReturned/onInstallSnapshotReturned, we SHOULD break out.
                        break;
                    }
                }
            }
        } finally {
            if (isLogDebugEnabled) {
                sb.append(", after processed, continue to send entries: ").append(continueSendEntries);
                LOG.debug(sb.toString());
            }
            if (continueSendEntries) {
                // unlock in sendEntries.
                r.sendEntries();
            }
        }
    }

    /**
     * Reset in-flight state.
     * 重置飞行者状态
     */
    void resetInflights() {
        // 这里增加了版本号
        this.version++;
        // 清空队列
        this.inflights.clear();
        // 清空闲置res
        this.pendingResponses.clear();
        final int rs = Math.max(this.reqSeq, this.requiredNextSeq);
        // 重置了 请求的序号 以及待处理的请求序号
        this.reqSeq = this.requiredNextSeq = rs;
        releaseReader();
    }

    /**
     * 当收到 appendEntry 的响应结果后触发
     * @param id
     * @param inflight
     * @param status
     * @param request
     * @param response
     * @param rpcSendTime
     * @param startTimeMs
     * @param r
     * @return
     */
    private static boolean onAppendEntriesReturned(final ThreadId id, final Inflight inflight, final Status status,
                                                   final AppendEntriesRequest request,
                                                   final AppendEntriesResponse response, final long rpcSendTime,
                                                   final long startTimeMs, final Replicator r) {
        // 这里是特殊情况 请求与本地存储的飞行对象起始偏移量不一致
        if (inflight.startIndex != request.getPrevLogIndex() + 1) {
            LOG.warn(
                "Replicator {} received invalid AppendEntriesResponse, in-flight startIndex={}, request prevLogIndex={}, reset the replicator state and probe again.",
                r, inflight.startIndex, request.getPrevLogIndex());
            r.resetInflights();
            r.state = State.Probe;
            // unlock id in sendEmptyEntries
            r.sendEmptyEntries(false);
            return false;
        }
        // record metrics
        if (request.getEntriesCount() > 0) {
            r.nodeMetrics.recordLatency("replicate-entries", Utils.monotonicMs() - rpcSendTime);
            r.nodeMetrics.recordSize("replicate-entries-count", request.getEntriesCount());
            r.nodeMetrics.recordSize("replicate-entries-bytes", request.getData() != null ? request.getData().size()
                : 0);
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node "). //
                append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                append(" received AppendEntriesResponse from "). //
                append(r.options.getPeerId()). //
                append(" prevLogIndex=").append(request.getPrevLogIndex()). //
                append(" prevLogTerm=").append(request.getPrevLogTerm()). //
                append(" count=").append(request.getEntriesCount());
        }
        // 代表向follower 写入数据时失败了
        if (!status.isOk()) {
            // If the follower crashes, any RPC to the follower fails immediately,
            // so we need to block the follower for a while instead of looping until
            // it comes back or be removed
            // dummy_id is unlock in block
            if (isLogDebugEnabled) {
                sb.append(" fail, sleep.");
                LOG.debug(sb.toString());
            }
            // 触发监听器
            notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
            if (++r.consecutiveErrorTimes % 10 == 0) {
                LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}", r.options.getPeerId(),
                    r.consecutiveErrorTimes, status);
            }
            // 只要有一个 请求失败了 之后的请求就没有必要处理了 因为很有可能本节点已经不再是leader了 就要抛弃这些数据
            r.resetInflights();
            r.state = State.Probe;
            // unlock in in block  在一定延时后 发起一次探测请求
            r.block(startTimeMs, status.getCode());
            return false;
        }
        // 代表本次请求成功 那么就可以清空累加的失败次数
        r.consecutiveErrorTimes = 0;
        if (!response.getSuccess()) {
            // 代表leader 已经发生变化了
            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ").append(response.getTerm()).append(" expect term ")
                        .append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                final NodeImpl node = r.options.getNode();
                // 代表需要同步最新leader 的数据
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                r.destroy();
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Leader receives higher term heartbeat_response from peer:%s", r.options.getPeerId()));
                // 这里返回false 代表不需要继续处理优先队列中的 res 了 因为本节点已经不再是leader 了
                return false;
            }
            if (isLogDebugEnabled) {
                sb.append(" fail, find nextIndex remote lastLogIndex ").append(response.getLastLogIndex())
                    .append(" local nextIndex ").append(r.nextIndex);
                LOG.debug(sb.toString());
            }
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            // Fail, reset the state to try again from nextIndex.
            // 代表其他原因出错
            r.resetInflights();
            // prev_log_index and prev_log_term doesn't match
            if (response.getLastLogIndex() + 1 < r.nextIndex) {
                LOG.debug("LastLogIndex at peer={} is {}", r.options.getPeerId(), response.getLastLogIndex());
                // The peer contains less logs than leader
                // 代表follower 期望收到的下个偏移量
                r.nextIndex = response.getLastLogIndex() + 1;
            } else {
                // The peer contains logs from old term which should be truncated,
                // decrease _last_log_at_peer by one to test the right index to keep
                if (r.nextIndex > 1) {
                    LOG.debug("logIndex={} dismatch", r.nextIndex);
                    r.nextIndex--;
                } else {
                    LOG.error("Peer={} declares that log at index=0 doesn't match, which is not supposed to happen",
                        r.options.getPeerId());
                }
            }
            // dummy_id is unlock in _send_heartbeat
            // 发起一次探测 如果本节点不再是 leader 不会发起探测 同时本复制机 也会被销毁
            r.sendEmptyEntries(false);
            return false;
        }
        if (isLogDebugEnabled) {
            sb.append(", success");
            LOG.debug(sb.toString());
        }
        // success
        if (response.getTerm() != r.options.getTerm()) {
            r.resetInflights();
            r.state = State.Probe;
            LOG.error("Fail, response term {} dismatch, expect term {}", response.getTerm(), r.options.getTerm());
            id.unlock();
            return false;
        }
        if (rpcSendTime > r.lastRpcSendTimestamp) {
            r.lastRpcSendTimestamp = rpcSendTime;
        }
        // 代表本次插入了 多少LogEntry 当触发 appendEntries 时 会限定一个偏移量范围 并从LogManager中找到对应的数据 填充到
        // emb 中这时 entriesCount 就对应取出来的数量
        final int entriesSize = request.getEntriesCount();
        // 代表非心跳请求 那么现在就是 往某个follower 写入数据 且成功提交了 这时投票箱就可以增加一票 当超过半数的时候本数据
        // 就是真正的提交成功
        if (entriesSize > 0) {
            // 针对该 投票对象 增加票数 只要超过半数就可以触发 commited 了 将数据持久化到 rocksDB
            r.options.getBallotBox().commitAt(r.nextIndex, r.nextIndex + entriesSize - 1, r.options.getPeerId());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Replicated logs in [{}, {}] to peer {}", r.nextIndex, r.nextIndex + entriesSize - 1,
                    r.options.getPeerId());
            }
        } else {
            // The request is probe request, change the state into Replicate.
            // 代表本次请求是一个探测请求 修改状态为 复制
            r.state = State.Replicate;
        }
        // nextIndex 对应leader 发送数据到 该follower 的 偏移量 (只有收到响应结果时该值才会推进)
        r.nextIndex += entriesSize;
        r.hasSucceeded = true;
        // 触发追赶逻辑
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        // dummy_id is unlock in _send_entries
        // TODO 待理解
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        return true;
    }

    /**
     * 填充公共字段
     * @param rb
     * @param prevLogIndex
     * @param isHeartbeat
     * @return
     */
    private boolean fillCommonFields(final AppendEntriesRequest.Builder rb, long prevLogIndex, final boolean isHeartbeat) {
        final long prevLogTerm = this.options.getLogManager().getTerm(prevLogIndex);
        if (prevLogTerm == 0 && prevLogIndex != 0) {
            if (!isHeartbeat) {
                Requires.requireTrue(prevLogIndex < this.options.getLogManager().getFirstLogIndex());
                LOG.debug("logIndex={} was compacted", prevLogIndex);
                return false;
            } else {
                // The log at prev_log_index has been compacted, which indicates
                // we is or is going to install snapshot to the follower. So we let
                // both prev_log_index and prev_log_term be 0 in the heartbeat
                // request so that follower would do nothing besides updating its
                // leader timestamp.
                prevLogIndex = 0;
            }
        }
        rb.setTerm(this.options.getTerm());
        rb.setGroupId(this.options.getGroupId());
        rb.setServerId(this.options.getServerId().toString());
        rb.setPeerId(this.options.getPeerId().toString());
        rb.setPrevLogIndex(prevLogIndex);
        rb.setPrevLogTerm(prevLogTerm);
        rb.setCommittedIndex(this.options.getBallotBox().getLastCommittedIndex());
        return true;
    }

    /**
     * 该方法会在 LogManager 中设置一个WaitMeta 对象 负责监听某个写入的index 并将数据传播到所有follower 上
     * @param nextWaitIndex
     */
    private void waitMoreEntries(final long nextWaitIndex) {
        try {
            LOG.debug("Node {} waits more entries", this.options.getNode().getNodeId());
            if (this.waitId >= 0) {
                return;
            }
            // 代表该对象是第几个等待对象 (从启动开始不断递增)
            this.waitId = this.options.getLogManager().wait(nextWaitIndex - 1,
                // arg 对应this.id
                (arg, errorCode) -> continueSending((ThreadId) arg, errorCode), this.id);
            // 修改当前状态 为 空闲状态
            this.statInfo.runningState = RunningState.IDLE;
        } finally {
            this.id.unlock();
        }
    }

    /**
     * Send as many requests as possible.  将当前囤积的数据都发送出去
     */
    void sendEntries() {
        boolean doUnlock = true;
        try {
            long prevSendIndex = -1;
            while (true) {
                // 在循环中不断调用该方法 拉取可以发送的下标
                final long nextSendingIndex = getNextSendIndex();
                if (nextSendingIndex > prevSendIndex) {
                    // 真正发送数据的方法
                    if (sendEntries(nextSendingIndex)) {
                        prevSendIndex = nextSendingIndex;
                    } else {
                        doUnlock = false;
                        // id already unlock in sendEntries when it returns false.
                        break;
                    }
                } else {
                    break;
                }
            }
        } finally {
            if (doUnlock) {
                this.id.unlock();
            }
        }

    }

    /**
     * Send log entries to follower, returns true when success, otherwise false and unlock the id.
     * 将指定偏移量前的数据发送到其他节点
     * @param nextSendingIndex next sending index
     * @return send result.
     */
    private boolean sendEntries(final long nextSendingIndex) {
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        // 插入公共字段 如果插入失败的话 就安装快照
        if (!fillCommonFields(rb, nextSendingIndex - 1, false)) {
            // unlock id in installSnapshot
            installSnapshot();
            return false;
        }

        // 该对象用于存放要传输的数据  在 bytebuffer 的基础上增加了 自动扩容能力
        ByteBufferCollector dataBuf = null;
        final int maxEntriesSize = this.raftOptions.getMaxEntriesSize();
        // 实际上是借助于 recycle 的一个bytebuffer List 该对象会存放 所有data 实体
        final RecyclableByteBufferList byteBufList = RecyclableByteBufferList.newInstance();
        try {
            for (int i = 0; i < maxEntriesSize; i++) {
                // 发送的数据 被包装成这种类型
                final RaftOutter.EntryMeta.Builder emb = RaftOutter.EntryMeta.newBuilder();
                // 填充 meta 对象 无法再添加时跳出该方法
                if (!prepareEntry(nextSendingIndex, i, emb, byteBufList)) {
                    break;
                }
                // em 可以包含conf 信息 也可以包含 data
                rb.addEntries(emb.build());
            }
            // 如果没有从logManager 中找到对应的数据  一般来说肯定会有数据 有可能是重启了 数据还没有持久化
            if (rb.getEntriesCount() == 0) {
                // 判断是否重启了 是的话就安装快照
                if (nextSendingIndex < this.options.getLogManager().getFirstLogIndex()) {
                    installSnapshot();
                    return false;
                }
                // _id is unlock in _wait_more
                // 往 LogManager 中再插入一个waitMeta 对象  当数据被写入后 继续触发该方法
                waitMoreEntries(nextSendingIndex);
                return false;
            }
            // 装载了 data
            if (byteBufList.getCapacity() > 0) {
                // 申请一个 byteBufferCollector 对象
                dataBuf = ByteBufferCollector.allocateByRecyclers(byteBufList.getCapacity());
                for (final ByteBuffer b : byteBufList) {
                    dataBuf.put(b);
                }
                final ByteBuffer buf = dataBuf.getBuffer();
                // 反转成读模式
                buf.flip();
                // 将数据序列化后 设置到rb 中
                rb.setData(ZeroByteStringHelper.wrap(buf));
            }
        } finally {
            RecycleUtil.recycle(byteBufList);
        }

        // 生成请求对象
        final AppendEntriesRequest request = rb.build();
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Node {} send AppendEntriesRequest to {} term {} lastCommittedIndex {} prevLogIndex {} prevLogTerm {} logIndex {} count {}",
                this.options.getNode().getNodeId(), this.options.getPeerId(), this.options.getTerm(),
                request.getCommittedIndex(), request.getPrevLogIndex(), request.getPrevLogTerm(), nextSendingIndex,
                request.getEntriesCount());
        }
        this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
        // prevLogIndex 就是 nextIndex - 1
        this.statInfo.firstLogIndex = rb.getPrevLogIndex() + 1;
        this.statInfo.lastLogIndex = rb.getPrevLogIndex() + rb.getEntriesCount();

        final Recyclable recyclable = dataBuf;
        final int v = this.version;
        final long monotonicSendTimeMs = Utils.monotonicMs();
        final int seq = getAndIncrementReqSeq();
        // opt中的peerId 对应某个follower 上 实际上有多个Replicator  他们交由ReplicatorGroup 来统一管理
        final Future<Message> rpcFuture = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(),
            request, -1, new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                @Override
                public void run(final Status status) {
                    // 归还分配出去的buffer
                    RecycleUtil.recycle(recyclable);
                    // 处理返回结果
                    onRpcReturned(Replicator.this.id, RequestType.AppendEntries, status, request, getResponse(), seq,
                        v, monotonicSendTimeMs);
                }

            });
        // 代表当前有个请求等待响应
        addInflight(RequestType.AppendEntries, nextSendingIndex, request.getEntriesCount(), request.getData().size(),
            seq, rpcFuture);
        return true;

    }

    /**
     * 发送心跳请求
     * @param id
     * @param closure
     */
    public static void sendHeartbeat(final ThreadId id, final RpcResponseClosure<AppendEntriesResponse> closure) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            Utils.runClosureInThread(closure, new Status(RaftError.EHOSTDOWN, "Peer %s is not connected", id));
            return;
        }
        //id unlock in send empty entries.
        //通过复制者对象 发送心跳
        r.sendEmptyEntries(true, closure);
    }

    private static void sendHeartbeat(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        // unlock in sendEmptyEntries
        r.sendEmptyEntries(true);
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTimeoutNow(final boolean unlockId, final boolean stopAfterFinish) {
        sendTimeoutNow(unlockId, stopAfterFinish, -1);
    }

    /**
     * 发送超时
     * @param unlockId
     * @param stopAfterFinish
     * @param timeoutMs
     */
    private void sendTimeoutNow(final boolean unlockId, final boolean stopAfterFinish, final int timeoutMs) {
        final TimeoutNowRequest.Builder rb = TimeoutNowRequest.newBuilder();
        rb.setTerm(this.options.getTerm());
        rb.setGroupId(this.options.getGroupId());
        rb.setServerId(this.options.getServerId().toString());
        rb.setPeerId(this.options.getPeerId().toString());
        try {
            if (!stopAfterFinish) {
                // This RPC is issued by transfer_leadership, save this call_id so that
                // the RPC can be cancelled by stop.
                this.timeoutNowInFly = timeoutNow(rb, false, timeoutMs);
                this.timeoutNowIndex = 0;
            } else {
                timeoutNow(rb, true, timeoutMs);
            }
        } finally {
            if (unlockId) {
                this.id.unlock();
            }
        }

    }

    private Future<Message> timeoutNow(final TimeoutNowRequest.Builder rb, final boolean stopAfterFinish,
                                       final int timeoutMs) {
        final TimeoutNowRequest request = rb.build();
        return this.rpcService.timeoutNow(this.options.getPeerId().getEndpoint(), request, timeoutMs,
            new RpcResponseClosureAdapter<TimeoutNowResponse>() {

                @Override
                public void run(final Status status) {
                    if (Replicator.this.id != null) {
                        onTimeoutNowReturned(Replicator.this.id, status, request, getResponse(), stopAfterFinish);
                    }
                }

            });
    }

    @SuppressWarnings("unused")
    static void onTimeoutNowReturned(final ThreadId id, final Status status, final TimeoutNowRequest request,
                                     final TimeoutNowResponse response, final boolean stopAfterFinish) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node "). //
                append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                append(" received TimeoutNowResponse from "). //
                append(r.options.getPeerId());
        }
        if (!status.isOk()) {
            if (isLogDebugEnabled) {
                sb.append(" fail:").append(status);
                LOG.debug(sb.toString());
            }
            notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
            if (stopAfterFinish) {
                r.notifyOnCaughtUp(RaftError.ESTOP.getNumber(), true);
                r.destroy();
            } else {
                id.unlock();
            }
            return;
        }
        if (isLogDebugEnabled) {
            sb.append(response.getSuccess() ? " success" : " fail");
            LOG.debug(sb.toString());
        }
        if (response.getTerm() > r.options.getTerm()) {
            final NodeImpl node = r.options.getNode();
            r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
            r.destroy();
            node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                "Leader receives higher term timeout_now_response from peer:%s", r.options.getPeerId()));
            return;
        }
        if (stopAfterFinish) {
            r.notifyOnCaughtUp(RaftError.ESTOP.getNumber(), true);
            r.destroy();
        } else {
            id.unlock();
        }

    }

    public static boolean stop(final ThreadId id) {
        id.setError(RaftError.ESTOP.getNumber());
        return true;
    }

    public static boolean join(final ThreadId id) {
        id.join();
        return true;
    }

    public static long getLastRpcSendTimestamp(final ThreadId id) {
        final Replicator r = (Replicator) id.getData();
        if (r == null) {
            return 0L;
        }
        return r.lastRpcSendTimestamp;
    }

    public static boolean transferLeadership(final ThreadId id, final long logIndex) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        // dummy is unlock in _transfer_leadership
        return r.transferLeadership(logIndex);
    }

    private boolean transferLeadership(final long logIndex) {
        if (this.hasSucceeded && this.nextIndex > logIndex) {
            // _id is unlock in _send_timeout_now
            this.sendTimeoutNow(true, false);
            return true;
        }
        // Register log_index so that _on_rpc_return trigger
        // _send_timeout_now if _next_index reaches log_index
        this.timeoutNowIndex = logIndex;
        this.id.unlock();
        return true;
    }

    public static boolean stopTransferLeadership(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        r.timeoutNowIndex = 0;
        id.unlock();
        return true;
    }

    /**
     * 发送超时请求
     * @param id
     * @param timeoutMs
     * @return
     */
    public static boolean sendTimeoutNowAndStop(final ThreadId id, final int timeoutMs) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        // id unlock in sendTimeoutNow
        r.sendTimeoutNow(true, true, timeoutMs);
        return true;
    }

    /**
     * 获取下一个 index
     * @param id
     * @return
     */
    public static long getNextIndex(final ThreadId id) {
        // 为该 复制对象上锁
        final Replicator r = (Replicator) id.lock();
        // 代表该对象已经关闭 返回0
        if (r == null) {
            return 0;
        }
        long nextIdx = 0;
        if (r.hasSucceeded) {
            // 将 r.nextIndex 作为结果
            nextIdx = r.nextIndex;
        }
        id.unlock();
        return nextIdx;
    }

}
