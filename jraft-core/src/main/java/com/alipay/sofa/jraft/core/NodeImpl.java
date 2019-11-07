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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.JRaftServiceFactory;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.ReadOnlyService;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.closure.SynchronizedClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.Ballot;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.BallotBoxOptions;
import com.alipay.sofa.jraft.option.BootstrapOptions;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.option.ReplicatorGroupOptions;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.rpc.impl.core.BoltRaftClientService;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.impl.LogManagerImpl;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.DisruptorMetricSet;
import com.alipay.sofa.jraft.util.JRaftServiceLoader;
import com.alipay.sofa.jraft.util.JRaftSignalHandler;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Platform;
import com.alipay.sofa.jraft.util.RepeatedTimer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SignalHelper;
import com.alipay.sofa.jraft.util.ThreadHelper;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * The raft replica node implementation.
 * node节点的默认实现
 * 在 raft 集群中 可以向 leader 或者follower 读取数据 因为follower 是通过同步 logEntry 来生成数据的 必然能保证一致性 只是存在脏读 也就是读取到过期的数据
 * 写操作只能针对 leader 节点 这个概念应该跟rocketMq 的 概念是一致的 只能往 主 broker 写入 可以从副节点读取 只是同步存在时间差 可能读取不到最新消息
 * 当单点发生故障时 这里有2种情况 leader or follower
 * 如果是 leader 出现问题 在 下次选举前不可写只能读取 数据 同时数据同步也会暂停 那么就会出现大量脏读 对应到 CP
 * 如果是 follower 出现问题 没有影响 只是针对该节点读取失败 需要配合重试机制从其他节点读取
 * 当超过半数节点故障时 整个group 不具备可用性 少数节点提供只读服务 但是数量不足以选举出新的leader  可以通过resetPeers 重置集群节点数量 一般不推荐使用
 * 当节点出现故障在重启时  如果开启快照 会从快照中加载数据 如果没有开启快照 通过重放本地所有日志恢复数据
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:26:51 PM
 */
public class NodeImpl implements Node, RaftServerService {

    private static final Logger                                            LOG                      = LoggerFactory
                                                                                                        .getLogger(NodeImpl.class);

    static {
        try {
            if (SignalHelper.supportSignal()) {
                // TODO support windows signal
                if (!Platform.isWindows()) {
                    final List<JRaftSignalHandler> handlers = JRaftServiceLoader.load(JRaftSignalHandler.class) //
                        .sort();
                    SignalHelper.addSignal(SignalHelper.SIG_USR2, handlers);
                }
            }
        } catch (final Throwable t) {
            LOG.error("Fail to add signal.", t);
        }
    }

    // Max retry times when applying tasks.
    private static final int                                               MAX_APPLY_RETRY_TIMES    = 3;

    /**
     * 代表全局范围内的活跃节点 单机 下可以创建多个node
     */
    public static final AtomicInteger                                      GLOBAL_NUM_NODES         = new AtomicInteger(
                                                                                                        0);

    /** Internal states */
    private final ReadWriteLock                                            readWriteLock            = new ReentrantReadWriteLock();
    protected final Lock                                                   writeLock                = this.readWriteLock
                                                                                                        .writeLock();
    protected final Lock                                                   readLock                 = this.readWriteLock
                                                                                                        .readLock();
    /**
     * 标记当前节点的状态 包含 是leader 正在重新选举  是 follower etc..
     */
    private volatile State                                                 state;
    private volatile CountDownLatch                                        shutdownLatch;
    private long                                                           currTerm;
    private volatile long                                                  lastLeaderTimestamp;
    /**
     * 代表该节点所在的组的 leader
     */
    private PeerId                                                         leaderId                 = new PeerId();
    /**
     * 在选举阶段 该node 选择的投票对象为
     */
    private PeerId                                                         votedId;
    private final Ballot                                                   voteCtx                  = new Ballot();
    private final Ballot                                                   prevVoteCtx              = new Ballot();
    private ConfigurationEntry                                             conf;
    private StopTransferArg                                                stopTransferArg;
    /** Raft group and node options and identifier */
    private final String                                                   groupId;
    private NodeOptions                                                    options;
    private RaftOptions                                                    raftOptions;
    /**
     * 作为 端点的地址
     */
    private final PeerId                                                   serverId;
    /** Other services */
    private final ConfigurationCtx                                         confCtx;
    private LogStorage                                                     logStorage;
    private RaftMetaStorage                                                metaStorage;
    /**
     * 存放回调对象的队列
     */
    private ClosureQueue                                                   closureQueue;
    private ConfigurationManager                                           configManager;
    private LogManager                                                     logManager;
    private FSMCaller                                                      fsmCaller;
    private BallotBox                                                      ballotBox;
    /**
     * 用于处理生成快照的对象
     */
    private SnapshotExecutor                                               snapshotExecutor;
    private ReplicatorGroup                                                replicatorGroup;
    private final List<Closure>                                            shutdownContinuations    = new ArrayList<>();
    /**
     * node 作为 client 的通信对象
     */
    private RaftClientService                                              rpcService;
    private ReadOnlyService                                                readOnlyService;
    /** Timers */
    private TimerManager                                                   timerManager;
    /**
     * 选举相关定时器
     */
    private RepeatedTimer                                                  electionTimer;
    /**
     * 拉票定时器
     */
    private RepeatedTimer                                                  voteTimer;
    /**
     * 作为leader 时 定时检查与其他 follower的联系 如果超过半数丢失了 清除 自己的leader 信息 之后其他节点由于心跳超时开始进入预投票阶段
     */
    private RepeatedTimer                                                  stepDownTimer;
    /**
     * 快照定时器 定期生成快照 并删除LogManager 的无效数据
     */
    private RepeatedTimer                                                  snapshotTimer;
    private ScheduledFuture<?>                                             transferTimer;
    private ThreadId                                                       wakingCandidate;
    /** Disruptor to run node service */
    private Disruptor<LogEntryAndClosure>                                  applyDisruptor;
    private RingBuffer<LogEntryAndClosure>                                 applyQueue;

    /** Metrics*/
    private NodeMetrics                                                    metrics;

    private NodeId                                                         nodeId;
    private JRaftServiceFactory                                            serviceFactory;

    /** ReplicatorStateListeners */
    private final CopyOnWriteArrayList<Replicator.ReplicatorStateListener> replicatorStateListeners = new CopyOnWriteArrayList<>();

    /**
     * Node service event.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:55 PM
     */
    private static class LogEntryAndClosure {
        LogEntry       entry;
        Closure        done;
        long           expectedTerm;
        CountDownLatch shutdownLatch;

        public void reset() {
            this.entry = null;
            this.done = null;
            this.expectedTerm = 0;
            this.shutdownLatch = null;
        }
    }

    /**
     * 事件工厂
     */
    private static class LogEntryAndClosureFactory implements EventFactory<LogEntryAndClosure> {

        @Override
        public LogEntryAndClosure newInstance() {
            return new LogEntryAndClosure();
        }
    }

    /**
     * Event handler.
     * 外部传来的task 都会被封装成  LogEntryAndClosure 对象
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:30:07 PM
     */
    private class LogEntryAndClosureHandler implements EventHandler<LogEntryAndClosure> {
        // task list for batch  内部存储了一组任务 在 生产者堆积任务时 会批量执行
        private final List<LogEntryAndClosure> tasks = new ArrayList<>(NodeImpl.this.raftOptions.getApplyBatch());

        @Override
        public void onEvent(final LogEntryAndClosure event, final long sequence, final boolean endOfBatch)
                                                                                                          throws Exception {
            // 如果设置了闭锁 代表该任务是调用 shutdown 发布的事件  该对象的闭锁 就是 node 所持有的那个
            if (event.shutdownLatch != null) {
                if (!this.tasks.isEmpty()) {
                    // 执行队列中所有任务
                    executeApplyingTasks(this.tasks);
                }
                // 在全局范围内 将活跃节点数量减少
                final int num = GLOBAL_NUM_NODES.decrementAndGet();
                LOG.info("The number of active nodes decrement to {}.", num);
                // 唤醒join 的主线程
                event.shutdownLatch.countDown();
                return;
            }

            this.tasks.add(event);
            if (this.tasks.size() >= NodeImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
                executeApplyingTasks(this.tasks);
                this.tasks.clear();
            }
        }
    }

    /**
     * Configuration commit context.
     * 该对象用于记录 当前集群的配置信息
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:38 PM
     */
    private static class ConfigurationCtx {
        enum Stage {
            STAGE_NONE, // none stage
            STAGE_CATCHING_UP, // the node is catching-up  代表该节点的配置在追赶leader
            STAGE_JOINT, // joint stage
            STAGE_STABLE // stable stage
        }

        /**
         * 本节点对象
         */
        final NodeImpl node;
        /**
         * 当前所处状态
         */
        Stage          stage;
        int            nchanges;
        long           version;
        List<PeerId>   newPeers    = new ArrayList<>();
        List<PeerId>   oldPeers    = new ArrayList<>();
        List<PeerId>   addingPeers = new ArrayList<>();
        Closure        done;

        public ConfigurationCtx(final NodeImpl node) {
            super();
            this.node = node;
            // 初始化时 版本为0 同时 stage 为 none
            this.stage = Stage.STAGE_NONE;
            this.version = 0;
            this.done = null;
        }

        /**
         * Start change configuration.
         */
        void start(final Configuration oldConf, final Configuration newConf, final Closure done) {
            if (isBusy()) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Already in busy stage."));
                }
                throw new IllegalStateException("Busy stage");
            }
            if (this.done != null) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Already have done closure."));
                }
                throw new IllegalArgumentException("Already have done closure");
            }
            this.done = done;
            this.stage = Stage.STAGE_CATCHING_UP;
            this.oldPeers = oldConf.listPeers();
            this.newPeers = newConf.listPeers();
            final Configuration adding = new Configuration();
            final Configuration removing = new Configuration();
            newConf.diff(oldConf, adding, removing);
            this.nchanges = adding.size() + removing.size();
            if (adding.isEmpty()) {
                nextStage();
                return;
            }
            this.addingPeers = adding.listPeers();
            LOG.info("Adding peers: {}.", this.addingPeers);
            for (final PeerId newPeer : this.addingPeers) {
                if (!this.node.replicatorGroup.addReplicator(newPeer)) {
                    LOG.error("Node {} start the replicator failed, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
                final OnCaughtUp caughtUp = new OnCaughtUp(this.node, this.node.currTerm, newPeer, this.version);
                final long dueTime = Utils.nowMs() + this.node.options.getElectionTimeoutMs();
                if (!this.node.replicatorGroup.waitCaughtUp(newPeer, this.node.options.getCatchupMargin(), dueTime,
                    caughtUp)) {
                    LOG.error("Node {} waitCaughtUp, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
            }
        }

        void onCaughtUp(final long version, final PeerId peer, final boolean success) {
            if (version != this.version) {
                return;
            }
            Requires.requireTrue(this.stage == Stage.STAGE_CATCHING_UP, "Stage is not in STAGE_CATCHING_UP");
            if (success) {
                this.addingPeers.remove(peer);
                if (this.addingPeers.isEmpty()) {
                    nextStage();
                    return;
                }
                return;
            }
            LOG.warn("Node {} fail to catch up peer {} when trying to change peers from {} to {}.",
                this.node.getNodeId(), peer, this.oldPeers, this.newPeers);
            reset(new Status(RaftError.ECATCHUP, "Peer %s failed to catch up.", peer));
        }

        void reset() {
            reset(null);
        }

        void reset(final Status st) {
            if (st != null && st.isOk()) {
                this.node.stopReplicator(this.newPeers, this.oldPeers);
            } else {
                this.node.stopReplicator(this.oldPeers, this.newPeers);
            }
            this.newPeers.clear();
            this.oldPeers.clear();
            this.addingPeers.clear();
            this.version++;
            this.stage = Stage.STAGE_NONE;
            this.nchanges = 0;
            if (this.done != null) {
                Utils.runClosureInThread(this.done, st != null ? st : new Status(RaftError.EPERM,
                    "Leader stepped down."));
                this.done = null;
            }
        }

        /**
         * Invoked when this node becomes the leader, write a configuration change log as the first log.
         */
        void flush(final Configuration conf, final Configuration oldConf) {
            Requires.requireTrue(!isBusy(), "Flush when busy");
            this.newPeers = conf.listPeers();
            if (oldConf == null || oldConf.isEmpty()) {
                this.stage = Stage.STAGE_STABLE;
                this.oldPeers = this.newPeers;
            } else {
                this.stage = Stage.STAGE_JOINT;
                this.oldPeers = oldConf.listPeers();
            }
            this.node.unsafeApplyConfiguration(conf, oldConf == null || oldConf.isEmpty() ? null : oldConf, true);
        }

        void nextStage() {
            Requires.requireTrue(isBusy(), "Not in busy stage");
            switch (this.stage) {
                case STAGE_CATCHING_UP:
                    if (this.nchanges > 1) {
                        this.stage = Stage.STAGE_JOINT;
                        this.node.unsafeApplyConfiguration(new Configuration(this.newPeers), new Configuration(
                            this.oldPeers), false);
                        return;
                    }
                    // Skip joint consensus since only one peers has been changed here. Make
                    // it a one-stage change to be compatible with the legacy
                    // implementation.
                case STAGE_JOINT:
                    this.stage = Stage.STAGE_STABLE;
                    this.node.unsafeApplyConfiguration(new Configuration(this.newPeers), null, false);
                    break;
                case STAGE_STABLE:
                    final boolean shouldStepDown = !this.newPeers.contains(this.node.serverId);
                    reset(new Status());
                    if (shouldStepDown) {
                        this.node.stepDown(this.node.currTerm, true, new Status(RaftError.ELEADERREMOVED,
                            "This node was removed."));
                    }
                    break;
                case STAGE_NONE:
                    // noinspection ConstantConditions
                    Requires.requireTrue(false, "Can't reach here");
                    break;
            }
        }

        boolean isBusy() {
            return this.stage != Stage.STAGE_NONE;
        }
    }

    public NodeImpl() {
        this(null, null);
    }

    /**
     * 通过 groupid 和 serverId 进行初始化
     * @param groupId
     * @param serverId
     */
    public NodeImpl(final String groupId, final PeerId serverId) {
        super();
        if (groupId != null) {
            // groupId 必须满足某种格式
            Utils.verifyGroupId(groupId);
        }
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        // 当前状态属于 未初始化
        this.state = State.STATE_UNINITIALIZED;
        // 当一个节点 初始化时 当前任期为0  会跟当前集群同步吗 如果一个节点出现异常重启后会以什么方式重新加入到该集群呢???
        this.currTerm = 0;
        // 将当前时间作为最后更新leader的时间
        updateLastLeaderTimestamp(Utils.monotonicMs());
        // 初始化 本节点的配置上下文
        this.confCtx = new ConfigurationCtx(this);
        // 这个字段应该是记录需要唤醒的目标 候选人
        this.wakingCandidate = null;
        // 当某个节点被创建时 更新全局节点总数  既然是全局范围内 肯定是在哪里 获取到了 本集群其他节点的信息
        final int num = GLOBAL_NUM_NODES.incrementAndGet();
        LOG.info("The number of active nodes increment to {}.", num);
    }

    /**
     * 初始化 快照存储相关的对象 会读取本地快照元数据文件
     * @return
     */
    private boolean initSnapshotStorage() {
        // 当快照文件夹 url 没有设置时 不再初始化该对象
        if (StringUtils.isEmpty(this.options.getSnapshotUri())) {
            LOG.warn("Do not set snapshot uri, ignore initSnapshotStorage.");
            return true;
        }
        // 该对象用来从leader 下载快照 或者生成自己的快照
        this.snapshotExecutor = new SnapshotExecutorImpl();
        // 设置相关属性用来初始化 executor
        final SnapshotExecutorOptions opts = new SnapshotExecutorOptions();
        opts.setUri(this.options.getSnapshotUri());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(this);
        opts.setLogManager(this.logManager);
        opts.setAddr(this.serverId != null ? this.serverId.getEndpoint() : null);
        opts.setInitTerm(this.currTerm);
        opts.setFilterBeforeCopyRemote(this.options.isFilterBeforeCopyRemote());
        // get snapshot throttle
        opts.setSnapshotThrottle(this.options.getSnapshotThrottle());
        return this.snapshotExecutor.init(opts);
    }

    /**
     * 当Node 节点初始化时 初始化整个存储模块
     * @return
     */
    private boolean initLogStorage() {
        // 确保状态机以及启动
        Requires.requireNonNull(this.fsmCaller, "Null fsm caller");
        // 默认创建 基于RocksDB 的存储器
        this.logStorage = this.serviceFactory.createLogStorage(this.options.getLogUri(), this.raftOptions);
        // 初始化日志管理器 外部通过该对象访问logStorage
        this.logManager = new LogManagerImpl();
        // 需要的属性通过生成一个 选项对象来配置
        final LogManagerOptions opts = new LogManagerOptions();
        opts.setLogEntryCodecFactory(this.serviceFactory.createLogEntryCodecFactory());
        opts.setLogStorage(this.logStorage);
        opts.setConfigurationManager(this.configManager);
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(this.metrics);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        opts.setRaftOptions(this.raftOptions);
        return this.logManager.init(opts);
    }

    /**
     * 初始化元数据存储
     * @return
     */
    private boolean initMetaStorage() {
        this.metaStorage = this.serviceFactory.createRaftMetaStorage(this.options.getRaftMetaUri(), this.raftOptions);
        RaftMetaStorageOptions opts = new RaftMetaStorageOptions();
        opts.setNode(this);
        // (如果已经存在元数据文件 就会从文件中加载数据来初始化)
        if (!this.metaStorage.init(opts)) {
            LOG.error("Node {} init meta storage failed, uri={}.", this.serverId, this.options.getRaftMetaUri());
            return false;
        }
        // 获取当前任期
        this.currTerm = this.metaStorage.getTerm();
        // 获取当前投票对象
        this.votedId = this.metaStorage.getVotedFor().copy();
        return true;
    }

    /**
     * 处理快照相关定时任务
     */
    private void handleSnapshotTimeout() {
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                return;
            }
        } finally {
            this.writeLock.unlock();
        }
        // do_snapshot in another thread to avoid blocking the timer thread.
        // 避免阻塞定时线程  在额外线程中执行  这里没有设置 回调函数
        Utils.runInThread(() -> doSnapshot(null));
    }

    /**
     * 处理选举任务  这里可能就类似于设置了一个标识 然后每次 leader发送心跳到其他节点时 标识就被重置 当某次 间隔时间内没有收到心跳 就代表可以准备开始投票了
     * 该标识就是 最后次收到leader 发送数据的时间戳  对应isCurrentLeaderValid
     */
    private void handleElectionTimeout() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 必须要follower
            if (this.state != State.STATE_FOLLOWER) {
                return;
            }
            // 如果当前leader 还在正常工作就不需要为别的节点投票了 就是看最后收到leader消息的时间戳是否在指定范围内
            if (isCurrentLeaderValid()) {
                return;
            }
            // 重置leader  同时会触发状态机的 stopFollow (由用户实现钩子)
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT, "Lost connection from leader %s.",
                this.leaderId));
            // 因为在 preVote 中会解锁 这里就不需要解锁了
            doUnlock = false;
            // 预投票
            preVote();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 初始化状态机调用者
     * @param bootstrapId
     * @return
     */
    private boolean initFSMCaller(final LogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance, bootstrapId={}.", bootstrapId);
            return false;
        }
        // 存放回调对象的队列
        this.closureQueue = new ClosureQueueImpl();
        final FSMCallerOptions opts = new FSMCallerOptions();
        // 设置一个终止的回调对象
        opts.setAfterShutdown(status -> afterShutdown());
        opts.setLogManager(this.logManager);
        opts.setFsm(this.options.getFsm());
        opts.setClosureQueue(this.closureQueue);
        opts.setNode(this);
        opts.setBootstrapId(bootstrapId);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        // 初始化状态机调用者
        return this.fsmCaller.init(opts);
    }

    private static class BootstrapStableClosure extends LogManager.StableClosure {

        private final SynchronizedClosure done = new SynchronizedClosure();

        public BootstrapStableClosure() {
            super(null);
        }

        public Status await() throws InterruptedException {
            return this.done.await();
        }

        @Override
        public void run(final Status status) {
            this.done.run(status);
        }
    }

    public boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        if (opts.getLastLogIndex() > 0 && (opts.getGroupConf().isEmpty() || opts.getFsm() == null)) {
            LOG.error("Invalid arguments for bootstrap, groupConf={}, fsm={}, lastLogIndex={}.", opts.getGroupConf(),
                opts.getFsm(), opts.getLastLogIndex());
            return false;
        }
        if (opts.getGroupConf().isEmpty()) {
            LOG.error("Bootstrapping an empty node makes no sense.");
            return false;
        }
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        this.serviceFactory = opts.getServiceFactory();
        // Term is not an option since changing it is very dangerous
        final long bootstrapLogTerm = opts.getLastLogIndex() > 0 ? 1 : 0;
        final LogId bootstrapId = new LogId(opts.getLastLogIndex(), bootstrapLogTerm);
        this.options = new NodeOptions();
        this.raftOptions = this.options.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.options.setFsm(opts.getFsm());
        this.options.setLogUri(opts.getLogUri());
        this.options.setRaftMetaUri(opts.getRaftMetaUri());
        this.options.setSnapshotUri(opts.getSnapshotUri());

        this.configManager = new ConfigurationManager();
        // Create fsmCaller at first as logManager needs it to report error
        this.fsmCaller = new FSMCallerImpl();

        if (!initLogStorage()) {
            LOG.error("Fail to init log storage.");
            return false;
        }
        if (!initMetaStorage()) {
            LOG.error("Fail to init meta storage.");
            return false;
        }
        if (this.currTerm == 0) {
            this.currTerm = 1;
            if (!this.metaStorage.setTermAndVotedFor(1, new PeerId())) {
                LOG.error("Fail to set term.");
                return false;
            }
        }

        if (opts.getFsm() != null && !initFSMCaller(bootstrapId)) {
            LOG.error("Fail to init fsm caller.");
            return false;
        }

        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.getId().setTerm(this.currTerm);
        entry.setPeers(opts.getGroupConf().listPeers());

        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);

        final BootstrapStableClosure bootstrapDone = new BootstrapStableClosure();
        this.logManager.appendEntries(entries, bootstrapDone);
        if (!bootstrapDone.await().isOk()) {
            LOG.error("Fail to append configuration.");
            return false;
        }

        if (opts.getLastLogIndex() > 0) {
            if (!initSnapshotStorage()) {
                LOG.error("Fail to init snapshot storage.");
                return false;
            }
            final SynchronizedClosure snapshotDone = new SynchronizedClosure();
            this.snapshotExecutor.doSnapshot(snapshotDone);
            if (!snapshotDone.await().isOk()) {
                LOG.error("Fail to save snapshot, status={}.", snapshotDone.getStatus());
                return false;
            }
        }

        if (this.logManager.getFirstLogIndex() != opts.getLastLogIndex() + 1) {
            throw new IllegalStateException("First and last log index mismatch");
        }
        if (opts.getLastLogIndex() > 0) {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex()) {
                throw new IllegalStateException("Last log index mismatch");
            }
        } else {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex() + 1) {
                throw new IllegalStateException("Last log index mismatch");
            }
        }

        return true;
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private int randomTimeout(final int timeoutMs) {
        return ThreadLocalRandom.current().nextInt(timeoutMs, timeoutMs + this.raftOptions.getMaxElectionDelayMs());
    }

    /**
     * 初始化 node 对象
     * @param opts 代表初始化需要的参数
     * @return
     */
    @Override
    public boolean init(final NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        // 该对象是一个工厂内 内部存放了 很多快捷创建对应对象的方法
        this.serviceFactory = opts.getServiceFactory();
        this.options = opts;
        this.raftOptions = opts.getRaftOptions();
        // 统计先不看
        this.metrics = new NodeMetrics(opts.isEnableMetrics());

        // 必须指定ip
        if (this.serverId.getIp().equals(Utils.IP_ANY)) {
            LOG.error("Node can't started from IP_ANY.");
            return false;
        }

        // 一般不是由用户直接创建node 的 而是先创建一个 RaftGroupService  并设置必要的参数 在启动时 本节点会自动设置到 nodeManager中
        if (!NodeManager.getInstance().serverExists(this.serverId.getEndpoint())) {
            LOG.error("No RPC server attached to, did you forget to call addService?");
            return false;
        }

        // 初始化内部的一个 ScheduledExecutorService
        this.timerManager = new TimerManager();
        if (!this.timerManager.init(this.options.getTimerPoolSize())) {
            LOG.error("Fail to init timer manager.");
            return false;
        }

        // 注意下面的定时任务 只是创建而没有执行 因为不同的任务对应不同的角色 在初始化阶段只需要开启election任务就可以了

        /**
         * 投票任务 在某个节点变成 候选人后该任务就会启动 而在确定leader时任务会停止
         * 就是为了避免在一轮中没有确定leader 需要一个机制来触发下次的投票
         * 以整个集群来将 当先发现自己变成候选人的节点向其他节点发起投票申请后 其他节点还会执行哪个检查心跳的定时任务吗
         * TODO 如果本轮没有选出leader 那么下一轮是只有 上次的几个候选人能进行参选 还是 所有follower 又在哪块逻辑体现???
         */
        this.voteTimer = new RepeatedTimer("JRaft-VoteTimer", this.options.getElectionTimeoutMs()) {

            @Override
            protected void onTrigger() {
                handleVoteTimeout();
            }

            // 调整下次触发的时间间隔
            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };
        /**
         * 对应follower 检测心跳的任务
         */
        this.electionTimer = new RepeatedTimer("JRaft-ElectionTimer", this.options.getElectionTimeoutMs()) {

            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };
        this.stepDownTimer = new RepeatedTimer("JRaft-StepDownTimer", this.options.getElectionTimeoutMs() >> 1) {

            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };
        this.snapshotTimer = new RepeatedTimer("JRaft-SnapshotTimer", this.options.getSnapshotIntervalSecs() * 1000) {

            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }
        };

        this.configManager = new ConfigurationManager();

        this.applyDisruptor = DisruptorBuilder.<LogEntryAndClosure> newInstance() //
            .setRingBufferSize(this.raftOptions.getDisruptorBufferSize()) //
            .setEventFactory(new LogEntryAndClosureFactory()) //
            .setThreadFactory(new NamedThreadFactory("JRaft-NodeImpl-Disruptor-", true)) //
            .setProducerType(ProducerType.MULTI) //
            .setWaitStrategy(new BlockingWaitStrategy()) //
            .build();
        // 设置事件处理器 专门处理写入日志的请求 用户的一切请求应该都是作为Log并只能写入到 leader中 应该就是对应这里
        this.applyDisruptor.handleEventsWith(new LogEntryAndClosureHandler());
        this.applyDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        // 返回任务队列用于添加任务
        this.applyQueue = this.applyDisruptor.start();
        if (this.metrics.getMetricRegistry() != null) {
            this.metrics.getMetricRegistry().register("jraft-node-impl-disruptor",
                new DisruptorMetricSet(this.applyQueue));
        }

        // 创建状态机对象
        this.fsmCaller = new FSMCallerImpl();
        // 初始化Log存储
        if (!initLogStorage()) {
            LOG.error("Node {} initLogStorage failed.", getNodeId());
            return false;
        }
        // 初始化元数据存储对象
        if (!initMetaStorage()) {
            LOG.error("Node {} initMetaStorage failed.", getNodeId());
            return false;
        }
        // 初始化状态机
        if (!initFSMCaller(new LogId(0, 0))) {
            LOG.error("Node {} initFSMCaller failed.", getNodeId());
            return false;
        }
        // 初始化投票箱对象
        this.ballotBox = new BallotBox();
        final BallotBoxOptions ballotBoxOpts = new BallotBoxOptions();
        ballotBoxOpts.setWaiter(this.fsmCaller);
        ballotBoxOpts.setClosureQueue(this.closureQueue);
        if (!this.ballotBox.init(ballotBoxOpts)) {
            LOG.error("Node {} init ballotBox failed.", getNodeId());
            return false;
        }

        if (!initSnapshotStorage()) {
            LOG.error("Node {} initSnapshotStorage failed.", getNodeId());
            return false;
        }

        // 检查下标是否正常
        final Status st = this.logManager.checkConsistency();
        if (!st.isOk()) {
            LOG.error("Node {} is initialized with inconsistent log, status={}.", getNodeId(), st);
            return false;
        }
        this.conf = new ConfigurationEntry();
        this.conf.setId(new LogId());
        // if have log using conf in log, else using conf in options
        if (this.logManager.getLastLogIndex() > 0) {
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
        } else {
            this.conf.setConf(this.options.getInitialConf());
        }

        // TODO RPC service and ReplicatorGroup is in cycle dependent, refactor it
        this.replicatorGroup = new ReplicatorGroupImpl();
        // 专门用于投票的client
        this.rpcService = new BoltRaftClientService(this.replicatorGroup);
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(this);
        rgOpts.setRaftRpcClientService(this.rpcService);
        rgOpts.setSnapshotStorage(this.snapshotExecutor != null ? this.snapshotExecutor.getSnapshotStorage() : null);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.timerManager);

        // Adds metric registry to RPC service.
        this.options.setMetricRegistry(this.metrics.getMetricRegistry());

        if (!this.rpcService.init(this.options)) {
            LOG.error("Fail to init rpc service.");
            return false;
        }
        this.replicatorGroup.init(new NodeId(this.groupId, this.serverId), rgOpts);

        this.readOnlyService = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions rosOpts = new ReadOnlyServiceOptions();
        rosOpts.setFsmCaller(this.fsmCaller);
        rosOpts.setNode(this);
        rosOpts.setRaftOptions(this.raftOptions);

        if (!this.readOnlyService.init(rosOpts)) {
            LOG.error("Fail to init readOnlyService.");
            return false;
        }

        // set state to follower
        this.state = State.STATE_FOLLOWER;

        if (LOG.isInfoEnabled()) {
            LOG.info("Node {} init, term={}, lastLogId={}, conf={}, oldConf={}.", getNodeId(), this.currTerm,
                this.logManager.getLastLogId(false), this.conf.getConf(), this.conf.getOldConf());
        }

        if (this.snapshotExecutor != null && this.options.getSnapshotIntervalSecs() > 0) {
            LOG.debug("Node {} start snapshot timer, term={}.", getNodeId(), this.currTerm);
            this.snapshotTimer.start();
        }

        if (!this.conf.isEmpty()) {
            stepDown(this.currTerm, false, new Status());
        }

        if (!NodeManager.getInstance().add(this)) {
            LOG.error("NodeManager add {} failed.", getNodeId());
            return false;
        }

        // Now the raft node is started , have to acquire the writeLock to avoid race
        // conditions
        this.writeLock.lock();
        if (this.conf.isStable() && this.conf.getConf().size() == 1 && this.conf.getConf().contains(this.serverId)) {
            // The group contains only this server which must be the LEADER, trigger
            // the timer immediately.
            electSelf();
        } else {
            this.writeLock.unlock();
        }

        return true;
    }

    /**
     * should be in writeLock
     * 将自身升级成候选人 同时为自己投票以及去其他节点拉票
     */
    private void electSelf() {
        long oldTerm;
        try {
            LOG.info("Node {} start vote and grant vote self, term={}.", getNodeId(), this.currTerm);
            // 如果本节点 不存在于当前配置中说明异常
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do electSelf as it is not in {}.", getNodeId(), this.conf);
                return;
            }
            // 如果是follower 进入这里代表 本节点需要晋升 就关闭选举任务 选举任务是用来判断收到心跳是否超时的
            // 已经变成候选人就不需要检测这个了只要想着投票就好 而follower直接进入该方法就说明已经确定leader失效
            // 比如通过了预投票阶段 或者触发了 更换leader
            if (this.state == State.STATE_FOLLOWER) {
                LOG.debug("Node {} stop election timer, term={}.", getNodeId(), this.currTerm);
                this.electionTimer.stop();
            }
            // 因为准备更换leader 了 所以重置leaderId
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT,
                "A follower's leader_id is reset to NULL as it begins to request_vote."));
            // 将自身状态修改为 候选人 并增加任期 代表进入了新一轮选举
            this.state = State.STATE_CANDIDATE;
            // 此时才增加任期 在预投票阶段是不增加任期的
            this.currTerm++;
            // 代表该对象投票节点是自身
            this.votedId = this.serverId.copy();
            LOG.debug("Node {} start vote timer, term={} .", getNodeId(), this.currTerm);
            // 开启投票定时器 因为 本轮投票不一定能选出leader 既然已经停止检测心跳了就需要一个别的触发点来 进行选举
            this.voteTimer.start();
            // 初始化一个投票对象  也就是logEntry的 投票和 选择leader 的投票走的是同一套体系
            this.voteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // 确认本follower(现在整个组中都没有leader了) 最后有效的偏移量  如果当前有未刷盘的数据 无论是否写入超过半数 都会被强制刷盘
        // 如果本节点写入了不存在的数据  投票的逻辑是怎样 需要确认一下 总之在这种情形下会强制刷盘未达到半数的数据
        final LogId lastLogId = this.logManager.getLastLogId(true);

        this.writeLock.lock();
        try {
            // vote need defense ABA after unlock&writeLock
            // 任期又发生了变化 就不需要往下执行了
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when getLastLogId.", getNodeId(), this.currTerm);
                return;
            }
            for (final PeerId peer : this.conf.listPeers()) {
                // 跳过自身 开始向其他节点拉票
                if (peer.equals(this.serverId)) {
                    continue;
                }
                // 连接失败的节点 也跳过 只要满足半数就可以
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                // 开始构建投票请求 推测回调成功 也是操纵ballot 对象 一旦 满足半数条件就升级成leader
                final OnRequestVoteRpcDone done = new OnRequestVoteRpcDone(peer, this.currTerm, this);
                done.request = RequestVoteRequest.newBuilder() //
                    // 本请求是否是一个前置请求
                    .setPreVote(false) // It's not a pre-vote request.
                    .setGroupId(this.groupId) //
                    // serverId 保留的是本候选人的信息
                    .setServerId(this.serverId.toString()) //
                    // 对端的peer信息
                    .setPeerId(peer.toString()) //
                    // 本轮投票对应的任期
                    .setTerm(this.currTerm) //
                    // 本节点最新写入的数据 注意 能够成功拉票跟该index判断有很大关系
                    .setLastLogIndex(lastLogId.getIndex()) //
                    .setLastLogTerm(lastLogId.getTerm()) //
                    .build();
                // 发起投票请求
                this.rpcService.requestVote(peer.getEndpoint(), done.request, done);
            }

            // 更改本节点的当前任期已经投选的节点 (实际上会写入到文件内)
            this.metaStorage.setTermAndVotedFor(this.currTerm, this.serverId);
            // 这里将自己的票放进投票对象中
            this.voteCtx.grant(this.serverId);
            if (this.voteCtx.isGranted()) {
                becomeLeader();
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 重置 Leader   如果主动调用某个 更改leader 的请求 也会重置 leaderId
     * @param newLeaderId  当本node作为follower 首次收到leader 发来的请求时 请求体中会携带 serverId 也就是leaderId
     *                     如果本节点没有设置 leaderId 那么就会被重置
     * @param status
     */
    private void resetLeaderId(final PeerId newLeaderId, final Status status) {
        // 代表要更换leader
        if (newLeaderId.isEmpty()) {
            // STATE_TRANSFERRING 以上就代表本节点不是leader 节点
            if (!this.leaderId.isEmpty() && this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                // 发出停止跟随某个 leader 的动作
                this.fsmCaller.onStopFollowing(new LeaderChangeContext(this.leaderId.copy(), this.currTerm, status));
            }
            this.leaderId = PeerId.emptyPeer();
        // 代表设置一个新的leader  当某个follower 接收到新的leader 发来的数据时 就会设置leaderId
        } else {
            if (this.leaderId == null || this.leaderId.isEmpty()) {
                // 开始跟随一个新的leader
                this.fsmCaller.onStartFollowing(new LeaderChangeContext(newLeaderId, this.currTerm, status));
            }
            this.leaderId = newLeaderId.copy();
        }
    }

    // in writeLock

    /**
     * 检验 leader 节点是否已经失去leaderShip
     * @param requestTerm  对应本次发起请求的leader 的任期
     * @param serverId
     */
    private void checkStepDown(final long requestTerm, final PeerId serverId) {
        final Status status = new Status();
        // 接收到来自更新的 leader 的数据  当之前的leader 被更换掉后 收到的新请求 任期自然是新的
        if (requestTerm > this.currTerm) {
            status.setError(RaftError.ENEWLEADER, "Raft node receives message from new leader with higher term.");
            stepDown(requestTerm, false, status);
        // 本节点是候选人时
        } else if (this.state != State.STATE_FOLLOWER) {
            status.setError(RaftError.ENEWLEADER, "Candidate receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        // 本节点还是 follower 但是长时间没有收到leader 的心跳
        } else if (this.leaderId.isEmpty()) {
            status.setError(RaftError.ENEWLEADER, "Follower receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        // stepDown 最后会清除 leaderId
        // save current leader 更新当前的leaderId
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            resetLeaderId(serverId, status);
        }
    }

    /**
     * 当该节点 拿到了足够的票数 并升级成 leader时
     */
    private void becomeLeader() {
        Requires.requireTrue(this.state == State.STATE_CANDIDATE, "Illegal state: " + this.state);
        LOG.info("Node {} become leader of group, term={}, conf={}, oldConf={}.", getNodeId(), this.currTerm,
            this.conf.getConf(), this.conf.getOldConf());
        // cancel candidate vote timer 如果当前已经变成leader 了就不需要再启动投票定时器了
        stopVoteTimer();
        this.state = State.STATE_LEADER;
        // 将leaderId 变成自己
        this.leaderId = this.serverId.copy();
        // 重置复制机管理者的 任期 如果当前节点变成了 leader 相当于要管理一个新的复制机 (如果之前是follower 是不需要做复制动作的)
        this.replicatorGroup.resetTerm(this.currTerm);
        // 将当前集群中所有节点 添加到复制机组中
        for (final PeerId peer : this.conf.listPeers()) {
            // 自身不需要被复制
            if (peer.equals(this.serverId)) {
                continue;
            }
            LOG.debug("Node {} add replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            // 应该是在 stepDown 中将 复制机组重置了吧  这里向所有follower 发送探测信息(主要是告诉他们产生了新的leader)
            if (!this.replicatorGroup.addReplicator(peer)) {
                LOG.error("Fail to add replicator, peer={}.", peer);
            }
        }
        // init commit manager
        // 初始化投票箱对象
        this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
        // Register _conf_ctx to reject configuration changing before the first log
        // is committed.  检测当前配置信息对象是否正处于其他状态 必须确保为 none  什么情况不为 none???
        if (this.confCtx.isBusy()) {
            throw new IllegalStateException();
        }
        // 将当前最新的配置 更新进去 一旦当某个节点变成leader 那么整个group 以它为基准
        this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());
        // 启动定期检查自己是否应该下线 的定时任务
        this.stepDownTimer.start();
    }

    // should be in writeLock

    /**
     * 实际上就是退位 根据当前角色 比如 当前是候选人 在收到 别的节点变成leader 后 就不需要触发有关候选人的定时任务了
     * @param term
     * @param wakeupCandidate
     * @param status  让位的原因
     */
    private void stepDown(final long term, final boolean wakeupCandidate, final Status status) {
        LOG.debug("Node {} stepDown, term={}, newTerm={}, wakeupCandidate={}.", getNodeId(), this.currTerm, term,
            wakeupCandidate);
        // 如果已经失活 就没有处理的必要了
        if (!this.state.isActive()) {
            return;
        }
        // 如果当前节点是 候选人 该场景对应于 收到了更新任期的请求 那么本节点所在任期的选举已经无效了 就停止选举
        if (this.state == State.STATE_CANDIDATE) {
            stopVoteTimer();
        // 如果本节点就是 leader  多个leader 之间会发送数据都会进入这个分支 那么应该根据 term 确定哪个才是该保留的
        // 难道说哪里能够保证 当前真正的leader 不会收到其他leader 发来的请求??? 这样就对应下面无条件关闭leader了
        } else if (this.state.compareTo(State.STATE_TRANSFERRING) <= 0) {
            // 本节点不再作为leader  关闭 stepdown 定时任务 也就是定期检查 有多少节点失效 超过半数 自行降级
            stopStepDownTimer();
            // 清理了投票箱 投票箱内还残存的是用户针对本leader 提交的任务 那么 看来用户在提交任务时 要根据对应回调被触发的时候再做处理会比较好 那时才是真正的写入
            this.ballotBox.clearPendingTasks();
            // signal fsm leader stop immediately
            // 如果当前节点是 leader 那么 立即触发 stop
            if (this.state == State.STATE_LEADER) {
                onLeaderStop(status);
            }
        }
        // reset leader_id
        resetLeaderId(PeerId.emptyPeer(), status);

        // soft state in memory
        // 强制设置为 跟随者
        this.state = State.STATE_FOLLOWER;
        // TODO 这个晚点看
        this.confCtx.reset();
        // 设置leader 更新时间
        updateLastLeaderTimestamp(Utils.monotonicMs());
        // 如果正在执行下载快照任务 要先进行打断 (就是 follower 从leader 拉取快照数据的动作)
        // 这招可以用  就是通过 异步执行一个任务(future)   正常情况下 任务完成 才设置需要的数据 而如果调用cancel 实际上
        // 主线程就无法得知任务究竟执行到哪步了 只能得到一个被关闭的结果 不会处在不确定的状态
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.interruptDownloadingSnapshots(term);
        }

        // meta state
        // 如果触发方法的节点 的任期更新
        if (term > this.currTerm) {
            this.currTerm = term;
            // 代表进入了下一轮那么置空votedId 作为下轮的投票节点字段
            this.votedId = PeerId.emptyPeer();
            // 重置 term 和 votedId 同时写入到文件
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        // 下面做了停止复制机组的操作

        // 如果需要唤醒候选人
        if (wakeupCandidate) {
            // 关闭除目标复制机外的其他机器
            this.wakingCandidate = this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
            if (this.wakingCandidate != null) {
                // 主动让该节点超时 原本情况是要 从节点自己执行那个预投票对应的定时任务去发现 与leader断开连接
                // 而且这样follower 会跳过预投票阶段直接进入投票阶段 就不需要判断 是否有半数未收到心跳
                // 在通知完候选节点后 再关闭本复制机
                Replicator.sendTimeoutNowAndStop(this.wakingCandidate, this.options.getElectionTimeoutMs());
            }
        } else {
            // 只要触发该方法 复制机就会被停止
            this.replicatorGroup.stopAll();
        }
        // TODO 这个待会看
        if (this.stopTransferArg != null) {
            if (this.transferTimer != null) {
                this.transferTimer.cancel(true);
            }
            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark stopTransferArg to NULL
            this.stopTransferArg = null;
        }
        // 本节点已经作为follower 了 那么开启检测心跳的定时任务
        this.electionTimer.start();
    }

    private void stopStepDownTimer() {
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
        }
    }

    /**
     * 当发现产生了新的 leader 时 就不需要开启投票了
     */
    private void stopVoteTimer() {
        if (this.voteTimer != null) {
            this.voteTimer.stop();
        }
    }

    /**
     * 每个 回调对象 对应一批待处理entry 如果node 发起下一次添加数据的请求 那么对应的entries会存放在另一个回调对象中
     */
    class LeaderStableClosure extends LogManager.StableClosure {

        public LeaderStableClosure(final List<LogEntry> entries) {
            super(entries);
        }

        // 该回调意味着 将数据写入leader 成功 这样就在投票箱中增加了一票 之后只要半数节点添加成功就能在 任务回调中设置success
        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                // firstLogIndex 对应在logManager中写入的起始下标 lastLogIndex 对应该批entry 的 长度
                NodeImpl.this.ballotBox.commitAt(this.firstLogIndex, this.firstLogIndex + this.nEntries - 1,
                    NodeImpl.this.serverId);
            } else {
                LOG.error("Node {} append [{}, {}] failed.", getNodeId(), this.firstLogIndex, this.firstLogIndex
                                                                                              + this.nEntries - 1);
            }
        }
    }

    /**
     * 执行队列中所有任务 (一般是 生产者堆积了大量未处理任务 否则都是单个任务)
     * @param tasks
     */
    private void executeApplyingTasks(final List<LogEntryAndClosure> tasks) {
        this.writeLock.lock();
        try {
            final int size = tasks.size();
            // 因为 只有leader 才允许写入事件   TODO 这里要注意 不同的 errorState closuer会以怎样的策略去应对
            if (this.state != State.STATE_LEADER) {
                final Status st = new Status();
                if (this.state != State.STATE_TRANSFERRING) {
                    st.setError(RaftError.EPERM, "Is not leader.");
                } else {
                    // 本节点刚晋升成leader 还没有通知其他节点
                    st.setError(RaftError.EBUSY, "Is transferring leadership.");
                }
                LOG.debug("Node {} can't apply, status={}.", getNodeId(), st);
                for (int i = 0; i < size; i++) {
                    // 使用指定status 触发回调函数   防止回调任务耗时过大 或者说 阻碍node本身的流程 使用额外的线程池去处理任务
                    Utils.runClosureInThread(tasks.get(i).done, st);
                }
                return;
            }
            final List<LogEntry> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final LogEntryAndClosure task = tasks.get(i);
                // 该写入任务预想的预期 与本预期不一致
                // 默认情况下 task 的任期为-1 除非指定
                if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                    LOG.debug("Node {} can't apply task whose expectedTerm={} doesn't match currTerm={}.", getNodeId(),
                        task.expectedTerm, this.currTerm);
                    if (task.done != null) {
                        final Status st = new Status(RaftError.EPERM, "expected_term=%d doesn't match current_term=%d",
                            task.expectedTerm, this.currTerm);
                        Utils.runClosureInThread(task.done, st);
                    }
                    continue;
                }
                // 开始向投票箱提交任务  也就是 只有半数(以上) 成功提交任务才返回正常提交
                // 因为集群内部可能发生变动 这里将变动前后的节点都传入了 看看它是如何应对的
                // 因为一开始pendingIndex =  会导致无法添加任务 并为回调对象设置错误结果  如果成功添加任务 应该是在某个定时器中做处理
                if (!this.ballotBox.appendPendingTask(this.conf.getConf(),
                    this.conf.isStable() ? null : this.conf.getOldConf(), task.done)) {
                    Utils.runClosureInThread(task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
                    continue;
                }
                // set task entry info before adding to list.
                // 为什么能确定数据一定是 data 类型呢 难道是根据执行的方法确定???
                task.entry.getId().setTerm(this.currTerm);
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
            }
            // 将数据添加到rocksDB 中纯异步框架的特点就是 回调环环相扣 也就是通过回调的叠加 在异步线程中做的处理越来越多 但是对主线程不影响
            // 该方法还没有进行commited 只有当写入半数时 才会触发commited
            // 每台机器都会commited 吗 怎么通知的呢 还有 有可能在commited 时失败吗
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
            // update conf.first
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Returns the node metrics.
     *
     * @return returns metrics of current node.
     */
    @Override
    public NodeMetrics getNodeMetrics() {
        return this.metrics;
    }

    /**
     * Returns the JRaft service factory for current node.
     *@since 1.2.6
     * @return the service factory
     */
    public JRaftServiceFactory getServiceFactory() {
        return this.serviceFactory;
    }

    @Override
    public void readIndex(final byte[] requestContext, final ReadIndexClosure done) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(done, new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(done, "Null closure");
        this.readOnlyService.addRequest(requestContext, done);
    }

    /**
     * ReadIndex response closure
     * @author dennis
     */
    private class ReadIndexHeartbeatResponseClosure extends RpcResponseClosureAdapter<AppendEntriesResponse> {
        final ReadIndexResponse.Builder             respBuilder;
        final RpcResponseClosure<ReadIndexResponse> closure;
        final int                                   quorum;
        final int                                   failPeersThreshold;
        int                                         ackSuccess;
        int                                         ackFailures;
        boolean                                     isDone;

        public ReadIndexHeartbeatResponseClosure(final RpcResponseClosure<ReadIndexResponse> closure,
                                                 final ReadIndexResponse.Builder rb, final int quorum,
                                                 final int peersCount) {
            super();
            this.closure = closure;
            this.respBuilder = rb;
            this.quorum = quorum;
            this.failPeersThreshold = peersCount % 2 == 0 ? (quorum - 1) : quorum;
            this.ackSuccess = 0;
            this.ackFailures = 0;
            this.isDone = false;
        }

        @Override
        public synchronized void run(final Status status) {
            if (this.isDone) {
                return;
            }
            if (status.isOk() && getResponse().getSuccess()) {
                this.ackSuccess++;
            } else {
                this.ackFailures++;
            }
            // Include leader self vote yes.
            if (this.ackSuccess + 1 >= this.quorum) {
                this.respBuilder.setSuccess(true);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            } else if (this.ackFailures >= this.failPeersThreshold) {
                this.respBuilder.setSuccess(false);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            }
        }
    }

    /**
     * Handle read index request.  处理readIndex 请求
     */
    @Override
    public void handleReadIndexRequest(final ReadIndexRequest request, final RpcResponseClosure<ReadIndexResponse> done) {
        final long startMs = Utils.monotonicMs();
        this.readLock.lock();
        try {
            switch (this.state) {
                case STATE_LEADER:
                    readLeader(request, ReadIndexResponse.newBuilder(), done);
                    break;
                case STATE_FOLLOWER:
                    readFollower(request, done);
                    break;
                case STATE_TRANSFERRING:
                    done.run(new Status(RaftError.EBUSY, "Is transferring leadership."));
                    break;
                default:
                    done.run(new Status(RaftError.EPERM, "Invalid state for readIndex: %s.", this.state));
                    break;
            }
        } finally {
            this.readLock.unlock();
            this.metrics.recordLatency("handle-read-index", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-read-index-entries", request.getEntriesCount());
        }
    }

    private int getQuorum() {
        final Configuration c = this.conf.getConf();
        if (c.isEmpty()) {
            return 0;
        }
        return c.getPeers().size() / 2 + 1;
    }

    private void readFollower(final ReadIndexRequest request, final RpcResponseClosure<ReadIndexResponse> closure) {
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            closure.run(new Status(RaftError.EPERM, "No leader at term %d.", this.currTerm));
            return;
        }
        // send request to leader.
        final ReadIndexRequest newRequest = ReadIndexRequest.newBuilder() //
            .mergeFrom(request) //
            .setPeerId(this.leaderId.toString()) //
            .build();
        this.rpcService.readIndex(this.leaderId.getEndpoint(), newRequest, -1, closure);
    }

    private void readLeader(final ReadIndexRequest request, final ReadIndexResponse.Builder respBuilder,
                            final RpcResponseClosure<ReadIndexResponse> closure) {
        final int quorum = getQuorum();
        if (quorum <= 1) {
            // Only one peer, fast path.
            respBuilder.setSuccess(true) //
                .setIndex(this.ballotBox.getLastCommittedIndex());
            closure.setResponse(respBuilder.build());
            closure.run(Status.OK());
            return;
        }

        final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
        if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
            // Reject read only request when this leader has not committed any log entry at its term
            closure
                .run(new Status(
                    RaftError.EAGAIN,
                    "ReadIndex request rejected because leader has not committed any log entry at its term, logIndex=%d, currTerm=%d.",
                    lastCommittedIndex, this.currTerm));
            return;
        }
        respBuilder.setIndex(lastCommittedIndex);

        if (request.getPeerId() != null) {
            // request from follower, check if the follower is in current conf.
            final PeerId peer = new PeerId();
            peer.parse(request.getServerId());
            if (!this.conf.contains(peer)) {
                closure
                    .run(new Status(RaftError.EPERM, "Peer %s is not in current configuration: {}.", peer, this.conf));
                return;
            }
        }

        ReadOnlyOption readOnlyOpt = this.raftOptions.getReadOnlyOptions();
        if (readOnlyOpt == ReadOnlyOption.ReadOnlyLeaseBased && !isLeaderLeaseValid()) {
            // If leader lease timeout, we must change option to ReadOnlySafe
            readOnlyOpt = ReadOnlyOption.ReadOnlySafe;
        }

        switch (readOnlyOpt) {
            case ReadOnlySafe:
                final List<PeerId> peers = this.conf.getConf().getPeers();
                Requires.requireTrue(peers != null && !peers.isEmpty(), "Empty peers");
                final ReadIndexHeartbeatResponseClosure heartbeatDone = new ReadIndexHeartbeatResponseClosure(closure,
                    respBuilder, quorum, peers.size());
                // Send heartbeat requests to followers
                for (final PeerId peer : peers) {
                    if (peer.equals(this.serverId)) {
                        continue;
                    }
                    this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
                }
                break;
            // 代表进一步追求性能 使用 续约机制
            // 原本 每个节点通过转发到leader 获取 readIndex 时 leader 需要向每个节点发送心跳确保自己仍是leader 而基于续约时间的策略代表
            // 每个 follower 是在一定时间内没有收到leader 的心跳自动升级为 candidator 那么 可以变相理解为每个leader 在上次发送心跳到 最短的follower检测心跳时间内
            // 自身都还会是 leader  那么就可以节省一次往其他follower 发送心跳的开销
            case ReadOnlyLeaseBased:
                // Responses to followers and local node.
                respBuilder.setSuccess(true);
                closure.setResponse(respBuilder.build());
                closure.run(Status.OK());
                break;
        }
    }

    /**
     * 在 rhea 模块中 任务首先提交到了statemachine 上 之后委托给raftRawKVStore 最后 转发到node上执行
     * 那么一般化来讲是什么样  还是通过状态机向node 发起任务???
     * 将任务提交到节点 这里必须确保本节点是 leader 否则不接受任务
     * @param task task to apply
     */
    @Override
    public void apply(final Task task) {
        // 只有调用shutdown时 shutdownLatch 会被设置  注意使用volatile 修饰确保可见性
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");

        // 提交的每个任务都会被封装成 LogEntry 写入到LogManager中 同时复制机 会将数据同步到其他节点
        // 在写入到 LogManager 中时 会判断当前预期写入的偏移量是否会超过 lastIndex 超过的话不会进行处理
        final LogEntry entry = new LogEntry();
        // data 是 序列化后的 kvOperation (针对hrea 模块来讲)
        entry.setData(task.getData());
        int retryTimes = 0;
        try {
            // 将 LogEntry 和 回调对象包装成一个对象
            final EventTranslator<LogEntryAndClosure> translator = (event, sequence) -> {
                // 因为 Disruptor 使用的对象并没有被回收 所以一般要配合 reset 将该对象重置 再设置必备的参数
                event.reset();
                event.done = task.getDone();
                event.entry = entry;
                event.expectedTerm = task.getExpectedTerm();
            };
            while (true) {
                // 往环形缓冲区中插入任务对象
                if (this.applyQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                        Utils.runClosureInThread(task.getDone(),
                            new Status(RaftError.EBUSY, "Node is busy, has too many tasks."));
                        LOG.warn("Node {} applyQueue is overload.", getNodeId());
                        this.metrics.recordTimes("apply-task-overload-times", 1);
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }

        } catch (final Exception e) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.EPERM, "Node is down."));
        }
    }

    /**
     * 进行预投票
     * @param request   data of the pre vote
     * @return
     */
    @Override
    public Message handlePreVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 本节点已经停止活动
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.",
                    getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received PreVoteRequest from {} serverId bad format.", getNodeId(),
                    request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse candidateId failed: %s.",
                    request.getServerId());
            }
            boolean granted = false;
            // noinspection ConstantConditions
            do {
                // 这里发现该节点的leader 还是有效的 那么很可能打算只是发起投票的 节点与leader  的通信断了
                // 如果本节点就是leader 的话  isCurrentLeaderValid 会是 false 就会进入到 } else if (request.getTerm() == this.currTerm + 1) {
                // 这里应该是在找未收到leader 的节点有多少吧 达到指定值 才能真正进入投票节点 要排除掉某个节点通信失败的情况
                if (this.leaderId != null && !this.leaderId.isEmpty() && isCurrentLeaderValid()) {
                    LOG.info(
                        "Node {} ignore PreVoteRequest from {}, term={}, currTerm={}, because the leader {}'s lease is still valid.",
                        getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, this.leaderId);
                    break;
                }
                // 请求节点的任期 旧了 那么本机如果是leader 就要通过复制机往对应节点发送心跳 来让对方察觉到 新的leader
                // 在投票环节 收到拉票请求的节点会将自身任期+1 且根据情况变更voteForId 但是leaderId 还没有变更
                // 那么修改的地方应该就是通过复制机 让对端收到了相同任期的请求  再修改leaderId 为 发出请求的peer
                if (request.getTerm() < this.currTerm) {
                    LOG.info("Node {} ignore PreVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    checkReplicator(candidateId);
                    break;
                // 这里是正常情况 因为在发起预投票时 client 还没有增加 任期 但是在请求体中发送的任期+1 也就超过同一时期其他节点的任期
                } else if (request.getTerm() == this.currTerm + 1) {
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    // check replicator state
                    // 如果本节点就是leader 节点那么 通过复制机重新往client 发起探测请求  注意针对这种情况 appendEntry 2端的 任期是一样的
                    checkReplicator(candidateId);
                }
                doUnlock = false;
                this.writeLock.unlock();

                // 获取本节点最新的下标
                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                // 如果对端的数据比这个旧 那么肯定是 它通信断了 所以不能让它变成候选人
                final LogId requestLastLogId = new LogId(request.getLastLogIndex(), request.getLastLogTerm());
                granted = requestLastLogId.compareTo(lastLogId) >= 0;

                LOG.info(
                    "Node {} received PreVoteRequest from {}, term={}, currTerm={}, granted={}, requestLastLogId={}, lastLogId={}.",
                    getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, granted, requestLastLogId,
                    lastLogId);
            } while (false);

            return RequestVoteResponse.newBuilder() //
                .setTerm(this.currTerm) //
                .setGranted(granted) //
                .build();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    // in read_lock
    private boolean isLeaderLeaseValid() {
        final long monotonicNowMs = Utils.monotonicMs();
        if (checkLeaderLease(monotonicNowMs)) {
            return true;
        }
        checkDeadNodes0(this.conf.getConf().getPeers(), monotonicNowMs, false, null);
        return checkLeaderLease(monotonicNowMs);
    }

    private boolean checkLeaderLease(final long monotonicNowMs) {
        return monotonicNowMs - this.lastLeaderTimestamp < this.options.getLeaderLeaseTimeoutMs();
    }

    /**
     * 判断当前 leader 能否正常工作  距离最后次收到 leader 的信息还没有超过选举的时间
     * @return
     */
    private boolean isCurrentLeaderValid() {
        return Utils.monotonicMs() - this.lastLeaderTimestamp < this.options.getElectionTimeoutMs();
    }

    /**
     * stepdown 定时任务每次都会检查本leader 管理的节点 有多少已经失效 在半数内 就会继续维持leadership
     * @param lastLeaderTimestamp
     */
    private void updateLastLeaderTimestamp(final long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    /**
     * 当本节点是leader 时检查状态
     * @param candidateId
     */
    private void checkReplicator(final PeerId candidateId) {
        if (this.state == State.STATE_LEADER) {
            this.replicatorGroup.checkReplicator(candidateId, false);
        }
    }

    /**
     * 处理拉票请求 在一个任期内 每个follower 应该只能给 一个候选人投票 这里要注意下是如何实现的
     * @param request   data of the vote
     * @return
     */
    @Override
    public Message handleRequestVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.",
                    getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            // 如果候选人传来的信息无效 那么只能返回异常
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received RequestVoteRequest from {} serverId bad format.", getNodeId(),
                    request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse candidateId failed: %s.",
                    request.getServerId());
            }

            // noinspection ConstantConditions
            do {
                // check term 此时该follower 还在上一轮中   因为先变成候选人的少数节点 负责通知其他还在前一任期的节点 当前已经进入下一轮了 需要增加任期
                if (request.getTerm() >= this.currTerm) {
                    LOG.info("Node {} received RequestVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    // increase current term, change state to follower  增加当前任期并变成follower
                    // 也有可能是上个任期中的leader 节点 因为脑裂 生成了新的leader 这时旧的leader 要变成follower
                    if (request.getTerm() > this.currTerm) {
                        stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                            "Raft node receives higher term RequestVoteRequest."));
                    }

                } else {
                    // ignore older term 如果因特殊原因导致收到了 旧的任期对应的数据 忽略
                    LOG.info("Node {} ignore RequestVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    break;
                }
                doUnlock = false;
                this.writeLock.unlock();

                // 刻意把幂等的操作移动到锁外
                // 开始对比最新的数据了 该方法会强制将投票未超过半数的数据刷盘
                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                // vote need ABA check after unlock&writeLock  再次确保 任期要相同
                if (request.getTerm() != this.currTerm) {
                    LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                    break;
                }

                // 这里捋一下  首先成为leader的条件是 拿到超过半数的票  而 写入成功代表着写入半数的follower 这里某个候选人 要比一半的人数据新或相同 才能拿到票
                // 也就是成为了 leader 那么 这些数据必然是有效的 提早的提交就没有问题(还没确认写入到半数节点)
                // 如果候选人的数据 落后于 投票者 那么 还是拿不到票 即使刷盘成功 后面新的leader 出来  还是要根据新的leader对象定义的起点偏移量写入数据 此时应该会覆盖已经刷盘的数据吧
                final boolean logIsOk = new LogId(request.getLastLogIndex(), request.getLastLogTerm())
                    .compareTo(lastLogId) >= 0;

                // 通过 votedId == null 来保证一个follower 在某个任期中只能投给一个候选人
                if (logIsOk && (this.votedId == null || this.votedId.isEmpty())) {
                    stepDown(request.getTerm(), false, new Status(RaftError.EVOTEFORCANDIDATE,
                        "Raft node votes for some candidate, step down to restart election_timer."));
                    this.votedId = candidateId.copy();
                    // 将本节点的投票对象设置到元数据中
                    this.metaStorage.setVotedFor(candidateId);
                }
            } while (false);

            return RequestVoteResponse.newBuilder() //
                .setTerm(this.currTerm) //
                    // 如果votedId 已经被设置的情况 这里就会返回false 代表本轮中该节点已经投给别人了
                .setGranted(request.getTerm() == this.currTerm && candidateId.equals(this.votedId)) //
                .build();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private static class FollowerStableClosure extends LogManager.StableClosure {

        final long                          committedIndex;
        final AppendEntriesResponse.Builder responseBuilder;
        final NodeImpl                      node;
        /**
         * 包含将结果发送到client 的回调
         */
        final RpcRequestClosure             done;
        final long                          term;

        public FollowerStableClosure(final AppendEntriesRequest request,
                                     final AppendEntriesResponse.Builder responseBuilder, final NodeImpl node,
                                     final RpcRequestClosure done, final long term) {
            super(null);
            this.committedIndex = Math.min(
            // committed index is likely less than the lastLogIndex
                request.getCommittedIndex(),
                // The logs after the appended entries can not be trust, so we can't commit them even if their indexes are less than request's committed index.
                request.getPrevLogIndex() + request.getEntriesCount());
            this.responseBuilder = responseBuilder;
            this.node = node;
            this.done = done;
            this.term = term;
        }

        /**
         * 当follower 写入 logEntry 并触发回调的时候执行
         * @param status the task status. 任务结果
         */
        @Override
        public void run(final Status status) {

            if (!status.isOk()) {
                // 异常情况
                // 实际上当前回调对象是 SequenceRpcRequestClosure
                this.done.run(status);
                return;
            }

            this.node.readLock.lock();
            try {
                // 如果在触发回调的时候 leader 发生了变化
                if (this.term != this.node.currTerm) {
                    // The change of term indicates that leader has been changed during
                    // appending entries, so we can't respond ok to the old leader
                    // because we are not sure if the appended logs would be truncated
                    // by the new leader:
                    //  - If they won't be truncated and we respond failure to the old
                    //    leader, the new leader would know that they are stored in this
                    //    peer and they will be eventually committed when the new leader
                    //    found that quorum of the cluster have stored.
                    //  - If they will be truncated and we responded success to the old
                    //    leader, the old leader would possibly regard those entries as
                    //    committed (very likely in a 3-nodes cluster) and respond
                    //    success to the clients, which would break the rule that
                    //    committed entries would never be truncated.
                    // So we have to respond failure to the old leader and set the new
                    // term to make it stepped down if it didn't.
                    // 这里返回失败且 包含了 该节点认为的最新的任期
                    this.responseBuilder.setSuccess(false).setTerm(this.node.currTerm);
                    this.done.sendResponse(this.responseBuilder.build());
                    return;
                }
            } finally {
                // It's safe to release lock as we know everything is ok at this point.
                this.node.readLock.unlock();
            }

            // Don't touch node any more.
            this.responseBuilder.setSuccess(true).setTerm(this.term);

            // Ballot box is thread safe and tolerates disorder.
            // 更新最后提交的偏移量
            this.node.ballotBox.setLastCommittedIndex(this.committedIndex);

            // 通过回调对象将结果返回到client
            this.done.sendResponse(this.responseBuilder.build());
        }
    }

    /**
     * server 接收从leader 传来的logEntry请求 到最后 还是委托给了node 来实现
     * @param request   data of the entries to append
     * @param done      callback  该回调内部嵌套了多层回调 包含 返回res给 client 的回调 和 client端处理res 的回调
     * @return
     */
    @Override
    public Message handleAppendEntriesRequest(final AppendEntriesRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        final long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        // 这个实际上是 conf 的数量  request.getData.size() 才是LogEntry的数量
        // 如果 不存在data 和 entries 代表本次是心跳请求
        final int entriesCount = request.getEntriesCount();
        try {
            // 如果节点已经失活了 返回无法正常处理请求的结果
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.",
                    getNodeId(), this.state.name());
            }

            final PeerId serverId = new PeerId();

            // serverId 是 leader 的id 而 peerId 就是本node的id (本node 是follower)
            if (!serverId.parse(request.getServerId())) {
                LOG.warn("Node {} received AppendEntriesRequest from {} serverId bad format.", getNodeId(),
                    request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse serverId failed: %s.",
                    request.getServerId());
            }

            // Check stale term  数据已经过期了
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale AppendEntriesRequest from {}, term={}, currTerm={}.", getNodeId(),
                    request.getServerId(), request.getTerm(), this.currTerm);
                return AppendEntriesResponse.newBuilder() //
                    .setSuccess(false) //
                    .setTerm(this.currTerm) //
                    .build();
            }

            // Check term and state to step down  这里在检测 是否要更新本节点的角色 可能是从候选人变成跟随者 里面还有些细节待梳理
            checkStepDown(request.getTerm(), serverId);
            // 代表leader发生了变化 TODO 这段先不看 主要先理清 logEntry 是如何添加到 follower 上的
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                    "More than one leader in the same term."));
                return AppendEntriesResponse.newBuilder() //
                    .setSuccess(false) //
                    .setTerm(request.getTerm() + 1) //
                    .build();
            }

            // 这里更新了最后收到心跳的时候 而每个follower 都会有一个选举的定时任务 通过检测在指定时间内没有收到leader 的心跳 尝试将自己升级成candidate
            updateLastLeaderTimestamp(Utils.monotonicMs());

            // entriesCount 是conf 的数量  data 才是LogEntry 的数量
            // 如果正在安装快照 不允许提交配置  如果本次集群复制失败了 那么怎么处理 代表提交失败了吗
            // 这里同时要求 entriesCount>0 应该是大于0才是正常的请求
            if (entriesCount > 0 && this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} received AppendEntriesRequest while installing snapshot.", getNodeId());
                return RpcResponseFactory.newResponse(RaftError.EBUSY, "Node %s:%s is installing snapshot.",
                    this.groupId, this.serverId);
            }

            // 获取上次写入 LogManager 的数据 以及对应的任期 这时数据可能还没持久化 而是仅仅写入了 leader的内存
            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            // ** 关键点 如果从follower上拉取数据对应的任期不对 则可能该follower 有部分数据尚未写入吗???
            // 没找到的情况返回0   2个任期会不匹配这时应该要同步阻塞安装快照???  用户是否必须要等 某个提交成功的数据 完全写入到所有节点才
            // 允许提交下个任务 应该是不会采用这么慢的方式 那么这里返回失败请求会如何处理呢 又或者中途更换了leader 那么 虽然找到了数据但是
            // 任期还是不匹配 如何处理???
            final long localPrevLogTerm = this.logManager.getTerm(prevLogIndex);
            if (localPrevLogTerm != prevLogTerm) {
                final long lastLogIndex = this.logManager.getLastLogIndex();

                LOG.warn(
                    "Node {} reject term_unmatched AppendEntriesRequest from {}, term={}, prevLogIndex={}, prevLogTerm={}, localPrevLogTerm={}, lastLogIndex={}, entriesSize={}.",
                    getNodeId(), request.getServerId(), request.getTerm(), prevLogIndex, prevLogTerm, localPrevLogTerm,
                    lastLogIndex, entriesCount);

                return AppendEntriesResponse.newBuilder() //
                    .setSuccess(false) //
                    .setTerm(this.currTerm) //
                    .setLastLogIndex(lastLogIndex) //
                    .build();
            }

            // 如果配置数量是0 可能代表本次请求实际上是一次心跳 也就是 follower 的投票箱是被动开启的 当集群中确定了第一个leader 后
            // leader 往其他节点发送心跳包 设置了其他follower的lastCommittedIndex (投票箱只有在 设置了该偏移量时才能正常使用)
            if (entriesCount == 0) {
                // heartbeat
                final AppendEntriesResponse.Builder respBuilder = AppendEntriesResponse.newBuilder() //
                    .setSuccess(true) //  这里设置success 为 true了
                    .setTerm(this.currTerm) //
                    .setLastLogIndex(this.logManager.getLastLogIndex());
                doUnlock = false;
                this.writeLock.unlock();
                // see the comments at FollowerStableClosure#run()
                this.ballotBox.setLastCommittedIndex(Math.min(request.getCommittedIndex(), prevLogIndex));
                return respBuilder.build();
            }

            // Parse request  代表是一次正常的请求
            long index = prevLogIndex;
            final List<LogEntry> entries = new ArrayList<>(entriesCount);
            ByteBuffer allData = null;
            if (request.hasData()) {
                allData = request.getData().asReadOnlyByteBuffer();
            }

            final List<RaftOutter.EntryMeta> entriesList = request.getEntriesList();
            for (int i = 0; i < entriesCount; i++) {
                final RaftOutter.EntryMeta entry = entriesList.get(i);
                index++;
                if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_UNKNOWN) {
                    // 将请求还原成 LogEntry
                    final LogEntry logEntry = new LogEntry();
                    logEntry.setId(new LogId(index, entry.getTerm()));
                    logEntry.setType(entry.getType());
                    if (entry.hasChecksum()) {
                        logEntry.setChecksum(entry.getChecksum()); // since 1.2.6
                    }
                    // 对应bytebuffer.remaining
                    final long dataLen = entry.getDataLen();
                    if (dataLen > 0) {
                        final byte[] bs = new byte[(int) dataLen];
                        assert allData != null;
                        // 将数据从bytebuffer 中 移动到 byte[]中 相当于deepcopy 了一份数据
                        allData.get(bs, 0, bs.length);
                        logEntry.setData(ByteBuffer.wrap(bs));
                    }

                    // 代表是conf 类型  将peerList 和 oldPeerList 转移到entry 中
                    if (entry.getPeersCount() > 0) {
                        if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                            throw new IllegalStateException(
                                "Invalid log entry that contains peers but is not ENTRY_TYPE_CONFIGURATION type: "
                                        + entry.getType());
                        }

                        final List<PeerId> peers = new ArrayList<>(entry.getPeersCount());
                        for (final String peerStr : entry.getPeersList()) {
                            final PeerId peer = new PeerId();
                            peer.parse(peerStr);
                            peers.add(peer);
                        }
                        logEntry.setPeers(peers);

                        if (entry.getOldPeersCount() > 0) {
                            final List<PeerId> oldPeers = new ArrayList<>(entry.getOldPeersCount());
                            for (final String peerStr : entry.getOldPeersList()) {
                                final PeerId peer = new PeerId();
                                peer.parse(peerStr);
                                oldPeers.add(peer);
                            }
                            logEntry.setOldPeers(oldPeers);
                        }
                    } else if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        throw new IllegalStateException(
                            "Invalid log entry that contains zero peers but is ENTRY_TYPE_CONFIGURATION type");
                    }

                    // Validate checksum
                    // 代表校验和验证失败
                    if (this.raftOptions.isEnableLogEntryChecksum() && logEntry.isCorrupted()) {
                        long realChecksum = logEntry.checksum();
                        LOG.error(
                            "Corrupted log entry received from leader, index={}, term={}, expectedChecksum={}, realChecksum={}",
                            logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                            realChecksum);
                        return RpcResponseFactory.newResponse(RaftError.EINVAL,
                            "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                            logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                            realChecksum);
                    }

                    entries.add(logEntry);
                }
            }

            // 构建了特殊的回调对象
            final FollowerStableClosure closure = new FollowerStableClosure(request, AppendEntriesResponse.newBuilder()
                    // 如果上面发现leader 发生了变化 currTerm 会更新成最新的任期
                .setTerm(this.currTerm), this, done, this.currTerm);
            // 同样只是写入到内存中异步刷盘 并在刷盘成功后触发回调
            // 该方法在leader 中触发回调会修改投票箱的数据 并触发replicator 的复制机制
            this.logManager.appendEntries(entries, closure);
            // update configuration after _log_manager updated its memory status
            // 更新配置信息
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
            return null;
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
            this.metrics.recordLatency("handle-append-entries", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-append-entries-count", entriesCount);
        }
    }

    // called when leader receive greater term in AppendEntriesResponse
    // 复制机会往其他follower 发送心跳 如果发现某个节点的任期更高 就会调用该方法
    void increaseTermTo(final long newTerm, final Status status) {
        this.writeLock.lock();
        try {
            if (newTerm < this.currTerm) {
                return;
            }
            stepDown(newTerm, false, status);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Peer catch up callback
     * 追赶任期的 回调对象
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:10:02 PM
     */
    private static class OnCaughtUp extends CatchUpClosure {
        private final NodeImpl node;
        private final long     term;
        private final PeerId   peer;
        private final long     version;

        public OnCaughtUp(final NodeImpl node, final long term, final PeerId peer, final long version) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
            this.version = version;
        }

        @Override
        public void run(final Status status) {
            this.node.onCaughtUp(this.peer, this.term, this.version, status);
        }
    }

    /**
     * 追赶任期
     * @param peer
     * @param term
     * @param version
     * @param st
     */
    private void onCaughtUp(final PeerId peer, final long term, final long version, final Status st) {
        this.writeLock.lock();
        try {
            // check current_term and state to avoid ABA problem
            if (term != this.currTerm && this.state != State.STATE_LEADER) {
                // term has changed and nothing should be done, otherwise there will be
                // an ABA problem.
                return;
            }
            if (st.isOk()) {
                // Caught up successfully
                this.confCtx.onCaughtUp(version, peer, true);
                return;
            }
            // Retry if this peer is still alive
            if (st.getCode() == RaftError.ETIMEDOUT.getNumber()
                && Utils.monotonicMs() - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= this.options
                    .getElectionTimeoutMs()) {
                LOG.debug("Node {} waits peer {} to catch up.", getNodeId(), peer);
                // 该回调对象是 当 本leader 发现自己已经不再是集群中最新任期时触发
                final OnCaughtUp caughtUp = new OnCaughtUp(this, term, peer, version);
                final long dueTime = Utils.nowMs() + this.options.getElectionTimeoutMs();
                if (this.replicatorGroup.waitCaughtUp(peer, this.options.getCatchupMargin(), dueTime, caughtUp)) {
                    return;
                }
                LOG.warn("Node {} waitCaughtUp failed, peer={}.", getNodeId(), peer);
            }
            LOG.warn("Node {} caughtUp failed, status={}, peer={}.", getNodeId(), st, peer);
            this.confCtx.onCaughtUp(version, peer, false);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 检测已经无效的节点
     * @param conf
     * @param monotonicNowMs
     */
    private void checkDeadNodes(final Configuration conf, final long monotonicNowMs) {
        // 获取当前集群内所有的节点
        final List<PeerId> peers = conf.listPeers();
        final Configuration deadNodes = new Configuration();
        // 检测成功直接返回  检测失败代表本leader 与半数以上的节点无法通信 那么需要对角色进行降级
        if (checkDeadNodes0(peers, monotonicNowMs, true, deadNodes)) {
            return;
        }
        LOG.warn("Node {} steps down when alive nodes don't satisfy quorum, term={}, deadNodes={}, conf={}.",
            getNodeId(), this.currTerm, deadNodes, conf);
        final Status status = new Status();
        // 设置尝试异常用于触发 stepdown
        status.setError(RaftError.ERAFTTIMEDOUT, "Majority of the group dies: %d/%d", deadNodes.size(), peers.size());
        stepDown(this.currTerm, false, status);
    }

    /**
     * 真正的检测逻辑
     * @param peers 本集群当前所有的节点
     * @param monotonicNowMs 开始检测的当前时间
     * @param checkReplicator
     * @param deadNodes 用于存放检测出来无效的节点
     * @return
     */
    private boolean checkDeadNodes0(final List<PeerId> peers, final long monotonicNowMs, final boolean checkReplicator,
                                    final Configuration deadNodes) {
        // 获取续约超时时间 如果上次收到响应结果超过这个时长 那么认为该节点已经离线
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        int aliveCount = 0;
        long startLease = Long.MAX_VALUE;
        for (final PeerId peer : peers) {
            // 跳过本节点 直接增加存活数量
            if (peer.equals(this.serverId)) {
                aliveCount++;
                continue;
            }
            // 如果要通过复制机检查  首先要确保自身是leader 复制机是什么时候被清除的还没确认 能进行该项检测的必然是 leader 吧 follower节点没有检查当前集群的必要
            if (checkReplicator) {
                checkReplicator(peer);
            }
            // 获取最后一次发往该节点并收到响应的时间 (每个复制机会定期发送心跳)
            final long lastRpcSendTimestamp = this.replicatorGroup.getLastRpcSendTimestamp(peer);
            // 这里简单讲就是将 最后一次响应的时间 与当前时间做对比 只有在 续约超时时间内 才算是存活
            if (monotonicNowMs - lastRpcSendTimestamp <= leaderLeaseTimeoutMs) {
                aliveCount++;
                if (startLease > lastRpcSendTimestamp) {
                    startLease = lastRpcSendTimestamp;
                }
                continue;
            }
            // 代表长时间掉线的节点会添加到该容器中
            if (deadNodes != null) {
                deadNodes.addPeer(peer);
            }
        }
        // 存活节点数超过 半数 那么该leader 就还生效  应该是这样的 发生网络分区时 发现半数无法通信上 那么对应的半数在心跳时间内没有收到leader的req 会尝试进行选举，所以本节点就不应该作为
        // leader 了 自动变更角色  而只要有半数 还能收到请求  剩余的节点即使发起 prevote 也无法成功
        if (aliveCount >= peers.size() / 2 + 1) {
            updateLastLeaderTimestamp(startLease);
            return true;
        }
        return false;
    }

    // in read_lock
    private List<PeerId> getAliveNodes(final List<PeerId> peers, final long monotonicNowMs) {
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        final List<PeerId> alivePeers = new ArrayList<>();
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                alivePeers.add(peer.copy());
                continue;
            }
            if (monotonicNowMs - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= leaderLeaseTimeoutMs) {
                alivePeers.add(peer.copy());
            }
        }
        return alivePeers;
    }

    /**
     * 处理 stepDown 任务
     */
    private void handleStepDownTimeout() {
        this.writeLock.lock();
        try {
            // 代表非 leader 的其他状态包含安装快照等
            if (this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.debug("Node {} stop step-down timer, term={}, state={}.", getNodeId(), this.currTerm, this.state);
                return;
            }
            // 获取当前时间 纳秒为单位
            final long monotonicNowMs = Utils.monotonicMs();
            // 检测无效的节点
            checkDeadNodes(this.conf.getConf(), monotonicNowMs);
            if (!this.conf.getOldConf().isEmpty()) {
                // 如果存在旧配置也检测一遍  2套配置会怎么样 存在2套leader吗???
                checkDeadNodes(this.conf.getOldConf(), monotonicNowMs);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configuration changed callback.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:53:43 PM
     */
    private class ConfigurationChangeDone implements Closure {
        private final long    term;
        private final boolean leaderStart;

        public ConfigurationChangeDone(final long term, final boolean leaderStart) {
            super();
            this.term = term;
            this.leaderStart = leaderStart;
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                onConfigurationChangeDone(this.term);
                if (this.leaderStart) {
                    getOptions().getFsm().onLeaderStart(this.term);
                }
            } else {
                LOG.error("Fail to run ConfigurationChangeDone, status: {}.", status);
            }
        }
    }

    private void unsafeApplyConfiguration(final Configuration newConf, final Configuration oldConf,
                                          final boolean leaderStart) {
        Requires.requireTrue(this.confCtx.isBusy(), "ConfigurationContext is not busy");
        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.setId(new LogId(0, this.currTerm));
        entry.setPeers(newConf.listPeers());
        if (oldConf != null) {
            entry.setOldPeers(oldConf.listPeers());
        }
        final ConfigurationChangeDone configurationChangeDone = new ConfigurationChangeDone(this.currTerm, leaderStart);
        // Use the new_conf to deal the quorum of this very log
        if (!this.ballotBox.appendPendingTask(newConf, oldConf, configurationChangeDone)) {
            Utils.runClosureInThread(configurationChangeDone, new Status(RaftError.EINTERNAL, "Fail to append task."));
            return;
        }
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
        this.conf = this.logManager.checkAndSetConfiguration(this.conf);
    }

    private void unsafeRegisterConfChange(final Configuration oldConf, final Configuration newConf, final Closure done) {
        if (this.state != State.STATE_LEADER) {
            LOG.warn("Node {} refused configuration changing as the state={}.", getNodeId(), this.state);
            if (done != null) {
                final Status status = new Status();
                if (this.state == State.STATE_TRANSFERRING) {
                    status.setError(RaftError.EBUSY, "Is transferring leadership.");
                } else {
                    status.setError(RaftError.EPERM, "Not leader");
                }
                Utils.runClosureInThread(done, status);
            }
            return;
        }
        // check concurrent conf change
        if (this.confCtx.isBusy()) {
            LOG.warn("Node {} refused configuration concurrent changing.", getNodeId());
            if (done != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Doing another configuration change."));
            }
            return;
        }
        // Return immediately when the new peers equals to current configuration
        if (this.conf.getConf().equals(newConf)) {
            Utils.runClosureInThread(done);
            return;
        }
        this.confCtx.start(oldConf, newConf, done);
    }

    /**
     * 当状态机终止时触发
     */
    private void afterShutdown() {
        List<Closure> savedDoneList = null;
        this.writeLock.lock();
        try {
            // 代表终止时仍要继续执行的任务
            if (!this.shutdownContinuations.isEmpty()) {
                savedDoneList = new ArrayList<>(this.shutdownContinuations);
            }
            if (this.logStorage != null) {
                this.logStorage.shutdown();
            }
            this.state = State.STATE_SHUTDOWN;
        } finally {
            this.writeLock.unlock();
        }
        if (savedDoneList != null) {
            for (final Closure closure : savedDoneList) {
                // 设置 state.ok 去触发回调
                Utils.runClosureInThread(closure);
            }
        }
    }

    @Override
    public NodeOptions getOptions() {
        return this.options;
    }

    public TimerManager getTimerManager() {
        return this.timerManager;
    }

    @Override
    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    @OnlyForTest
    long getCurrentTerm() {
        this.readLock.lock();
        try {
            return this.currTerm;
        } finally {
            this.readLock.unlock();
        }
    }

    @OnlyForTest
    ConfigurationEntry getConf() {
        this.readLock.lock();
        try {
            return this.conf;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        shutdown(null);
    }

    public void onConfigurationChangeDone(final long term) {
        this.writeLock.lock();
        try {
            if (term != this.currTerm || this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.warn("Node {} process onConfigurationChangeDone at term {} while state={}, currTerm={}.",
                    getNodeId(), term, this.state, this.currTerm);
                return;
            }
            this.confCtx.nextStage();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 获取当前 推举为leader  的节点的 peerId
     * @return
     */
    @Override
    public PeerId getLeaderId() {
        this.readLock.lock();
        try {
            return this.leaderId.isEmpty() ? null : this.leaderId;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public String getGroupId() {
        return this.groupId;
    }

    public PeerId getServerId() {
        return this.serverId;
    }

    @Override
    public NodeId getNodeId() {
        if (this.nodeId == null) {
            this.nodeId = new NodeId(this.groupId, this.serverId);
        }
        return this.nodeId;
    }

    public RaftClientService getRpcService() {
        return this.rpcService;
    }

    public void onError(final RaftException error) {
        LOG.warn("Node {} got error: {}.", getNodeId(), error);
        if (this.fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            this.fsmCaller.onError(error);
        }
        this.writeLock.lock();
        try {
            // If it is leader, need to wake up a new one;
            // If it is follower, also step down to call on_stop_following.
            if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                stepDown(this.currTerm, this.state == State.STATE_LEADER, new Status(RaftError.EBADNODE,
                    "Raft node(leader or candidate) is in error."));
            }
            if (this.state.compareTo(State.STATE_ERROR) < 0) {
                this.state = State.STATE_ERROR;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 处理本次有关投票请求的响应结果
     * @param peerId
     * @param term
     * @param response
     */
    public void handleRequestVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        this.writeLock.lock();
        try {
            // 如果当前不是候选人 那么不应该处理本次请求
            if (this.state != State.STATE_CANDIDATE) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, state not in STATE_CANDIDATE but {}.",
                    getNodeId(), peerId, this.state);
                return;
            }
            // check stale term
            // 如果返回的任期发生了变化 本次投票已经不作数了
            if (term != this.currTerm) {
                LOG.warn("Node {} received stale RequestVoteResponse from {}, term={}, currTerm={}.", getNodeId(),
                    peerId, term, this.currTerm);
                return;
            }
            // check response term
            // 代表对端的任期更高 将本节点角色修改(降级)
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, term={}, expect={}.", getNodeId(),
                    peerId, response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Raft node receives higher term request_vote_response."));
                return;
            }
            // check granted quorum? 是否成功获得选票
            if (response.getGranted()) {
                this.voteCtx.grant(peerId);
                if (this.voteCtx.isGranted()) {
                    becomeLeader();
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 拉票的请求回调
     */
    private class OnRequestVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long         startMs;
        final PeerId       peer;
        final long         term;
        final NodeImpl     node;
        /**
         * 内部维护了本次拉票的请求信息
         */
        RequestVoteRequest request;

        /**
         *
         * @param peer  对端信息
         * @param term  当前新的任期
         * @param node
         */
        public OnRequestVoteRpcDone(final PeerId peer, final long term, final NodeImpl node) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
            this.node = node;
        }

        @Override
        public void run(final Status status) {
            NodeImpl.this.metrics.recordLatency("request-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} RequestVote to {} error: {}.", this.node.getNodeId(), this.peer, status);
            } else {
                // 代表本次请求成功 开始处理拉票成功的情况 应该就是修改 ballot
                this.node.handleRequestVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    /**
     * 处理预投票请求
     * @param peerId
     * @param term
     * @param response
     */
    public void handlePreVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 确保当前角色是 follower 因为可能响应结果 滞后返回 那么角色已经变了就没有处理的必要了
            if (this.state != State.STATE_FOLLOWER) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, state not in STATE_FOLLOWER but {}.",
                    getNodeId(), peerId, this.state);
                return;
            }
            // 在同一任期才有处理的必要
            if (term != this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term={}, currTerm={}.", getNodeId(),
                    peerId, term, this.currTerm);
                return;
            }
            // 代表对端已经进入下一轮选举了
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term {}, expect={}.", getNodeId(), peerId,
                    response.getTerm(), this.currTerm);
                // follower 也可以调用该方法
                stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Raft node receives higher term pre_vote_response."));
                return;
            }
            LOG.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getNodeId(), peerId,
                response.getTerm(), response.getGranted());
            // check granted quorum?
            // 只有当某个节点 往其他节点发送预投票请求 并且得到的回应是 有半数都没有按时收到leader 心跳 本次预投票才算成功
            // 然后成功的几个节点会将自己变成候选人  如果小于半数很可能只是 该节点自己与leader 通信断了 当真正的leader 收到预投票请求
            // 后会通过复制机对该节点进行重连
            if (response.getGranted()) {
                this.prevVoteCtx.grant(peerId);
                if (this.prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    electSelf();
                }
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private class OnPreVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long         startMs;
        /**
         * 对端节点
         */
        final PeerId       peer;
        /**
         * 存放发送本次 预投票请求时的任期
         */
        final long         term;
        /**
         * 本次请求对象 内部的任期是  term+1
         */
        RequestVoteRequest request;

        public OnPreVoteRpcDone(final PeerId peer, final long term) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
        }

        @Override
        public void run(final Status status) {
            NodeImpl.this.metrics.recordLatency("pre-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} PreVote to {} error: {}.", getNodeId(), this.peer, status);
            } else {
                // 处理响应结果
                handlePreVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    /**
     * 预投票  in writeLock
     * 当某个follower在超过检测时间 还没有收到leader 的新数据时触发
     */
    private void preVote() {
        long oldTerm;
        try {
            LOG.info("Node {} term {} start preVote.", getNodeId(), this.currTerm);
            // 如果正巧在安装快照 先不进行投票  实际上即使投票了 因为数据不够新很有可能获取不到足够的票数导致失败
            if (this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn(
                    "Node {} term {} doesn't do preVote when installing snapshot as the configuration may be out of date.",
                    getNodeId());
                return;
            }
            // 如果该节点不属于新的配置中 也就是该节点从集群中被剔除了 不具备投票的条件  这里针对的是conf 发生变化
            // TODO 之后要考虑有关这里的逻辑
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do preVote as it is not in conf <{}>.", getNodeId(), this.conf);
                return;
            }
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // flush 代表本次要将之前未刷盘的数据全部刷盘 且返回最后一个刷盘成功的数据的 LogId
        final LogId lastLogId = this.logManager.getLastLogId(true);

        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // pre_vote need defense ABA after unlock&writeLock
            // 在等待刷盘的时候 已经收到了其他leader的请求 同时更新了 任期 那么就不需要发起请求了
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                return;
            }
            // 初始化一个投票上下文
            this.prevVoteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            // 获取所有节点
            for (final PeerId peer : this.conf.listPeers()) {
                // 跳过本节点
                if (peer.equals(this.serverId)) {
                    continue;
                }
                // 尝试连接到对应节点
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                // 本次请求会触发预投票的回调
                final OnPreVoteRpcDone done = new OnPreVoteRpcDone(peer, this.currTerm);
                done.request = RequestVoteRequest.newBuilder() //
                    .setPreVote(true) // it's a pre-vote request.
                    .setGroupId(this.groupId) //
                    .setServerId(this.serverId.toString()) //
                    .setPeerId(peer.toString()) // 代表其他follower的信息
                    .setTerm(this.currTerm + 1) // next term 注意这里选择的任期 是 下一次的
                    .setLastLogIndex(lastLogId.getIndex()) //
                    .setLastLogTerm(lastLogId.getTerm()) //
                    .build();
                this.rpcService.preVote(peer.getEndpoint(), done.request, done);
            }
            // 这里预投票成功是 将自己设置为候选人 而在 选举阶段投票成功是将自己设置成leader
            this.prevVoteCtx.grant(this.serverId);
            if (this.prevVoteCtx.isGranted()) {
                doUnlock = false;
                electSelf();
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 处理投票的定时任务   如果在集群中出现了某个 leader 那么该定时器将停止工作
     * 该任务应该是定期发现自己是 候选人 并在投票给自己后向集群中拉票
     * 这个动作不应该放在 当节点长时间没有收到leader 的心跳后 变成候选人并立即触发吗 而是开一个定时任务 那如果定时任务过久 会出现问题吧
     * 比如本有一个节点会最先变成候选人 但是在等待 投票的过程 中又额外产生了几个候选人
     */
    private void handleVoteTimeout() {
        this.writeLock.lock();
        // 如果当前自身是候选人 投自己
        if (this.state == State.STATE_CANDIDATE) {
            LOG.debug("Node {} term {} retry elect.", getNodeId(), this.currTerm);
            electSelf();
        } else {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean isLeader() {
        this.readLock.lock();
        try {
            return this.state == State.STATE_LEADER;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 停止该节点的工作
     * @param done callback
     */
    @Override
    public void shutdown(final Closure done) {
        List<RepeatedTimer> timers = null;
        this.writeLock.lock();
        try {
            LOG.info("Node {} shutdown, currTerm={} state={}.", getNodeId(), this.currTerm, this.state);
            if (this.state.compareTo(State.STATE_SHUTTING) < 0) {
                // 将本节点从 nodeManager 中移除
                NodeManager.getInstance().remove(this);
                // If it is leader, set the wakeup_a_candidate with true;
                // If it is follower, call on_stop_following in step_down
                if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                    stepDown(this.currTerm, this.state == State.STATE_LEADER,
                            new Status(RaftError.ESHUTDOWN, "Raft node is going to quit."));
                }
                this.state = State.STATE_SHUTTING;
                // Stop all timers
                timers = stopAllTimers();
                if (this.readOnlyService != null) {
                    this.readOnlyService.shutdown();
                }
                if (this.logManager != null) {
                    this.logManager.shutdown();
                }
                if (this.metaStorage != null) {
                    this.metaStorage.shutdown();
                }
                if (this.snapshotExecutor != null) {
                    this.snapshotExecutor.shutdown();
                }
                if (this.wakingCandidate != null) {
                    Replicator.stop(this.wakingCandidate);
                }
                if (this.fsmCaller != null) {
                    this.fsmCaller.shutdown();
                }
                if (this.rpcService != null) {
                    this.rpcService.shutdown();
                }
                // 如果发现了 环形缓冲区还存在 这里添加一个携带闭锁的event 对象 使得node 可以阻塞直到全部任务被处理完
                // 如果不使用闭锁 无法预测 ringBuffer 中的任务什么时候被处理完
                if (this.applyQueue != null) {
                    Utils.runInThread(() -> {
                        // 该对象与 event 持有同一个 闭锁 这样只有在处理事件后 闭锁才会被释放 那么调用 join的线程就会被唤醒
                        this.shutdownLatch = new CountDownLatch(1);
                        this.applyQueue.publishEvent((event, sequence) -> event.shutdownLatch = this.shutdownLatch);
                    });
                } else {
                    final int num = GLOBAL_NUM_NODES.decrementAndGet();
                    LOG.info("The number of active nodes decrement to {}.", num);
                }
                if (this.timerManager != null) {
                    this.timerManager.shutdown();
                }
            }

            if (this.state != State.STATE_SHUTDOWN) {
                if (done != null) {
                    this.shutdownContinuations.add(done);
                }
                return;
            }

            // This node is down, it's ok to invoke done right now. Don't invoke this
            // in place to avoid the dead writeLock issue when done.Run() is going to acquire
            // a writeLock which is already held by the caller
            if (done != null) {
                Utils.runClosureInThread(done);
            }
        } finally {
            this.writeLock.unlock();

            // Destroy all timers out of lock
            if (timers != null) {
                destroyAllTimers(timers);
            }
        }
    }

    // Should in lock
    private List<RepeatedTimer> stopAllTimers() {
        final List<RepeatedTimer> timers = new ArrayList<>();
        if (this.electionTimer != null) {
            this.electionTimer.stop();
            timers.add(this.electionTimer);
        }
        if (this.voteTimer != null) {
            this.voteTimer.stop();
            timers.add(this.voteTimer);
        }
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
            timers.add(this.stepDownTimer);
        }
        if (this.snapshotTimer != null) {
            this.snapshotTimer.stop();
            timers.add(this.snapshotTimer);
        }
        return timers;
    }

    private void destroyAllTimers(final List<RepeatedTimer> timers) {
        for (final RepeatedTimer timer : timers) {
            timer.destroy();
        }
    }

    /**
     * 阻塞等待shutdown 结束
     * @throws InterruptedException
     */
    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            if (this.readOnlyService != null) {
                this.readOnlyService.join();
            }
            if (this.fsmCaller != null) {
                this.fsmCaller.join();
            }
            if (this.logManager != null) {
                this.logManager.join();
            }
            if (this.snapshotExecutor != null) {
                this.snapshotExecutor.join();
            }
            if (this.wakingCandidate != null) {
                Replicator.join(this.wakingCandidate);
            }
            this.shutdownLatch.await();
            this.applyDisruptor.shutdown();
            this.shutdownLatch = null;
        }
    }

    private static class StopTransferArg {
        final NodeImpl node;
        final long     term;
        final PeerId   peer;

        public StopTransferArg(final NodeImpl node, final long term, final PeerId peer) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
        }
    }

    private void handleTransferTimeout(final long term, final PeerId peer) {
        LOG.info("Node {} failed to transfer leadership to peer {}, reached timeout.", getNodeId(), peer);
        this.writeLock.lock();
        try {
            if (term == this.currTerm) {
                this.replicatorGroup.stopTransferLeadership(peer);
                if (this.state == State.STATE_TRANSFERRING) {
                    this.fsmCaller.onLeaderStart(term);
                    this.state = State.STATE_LEADER;
                    this.stopTransferArg = null;
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private void onTransferTimeout(final StopTransferArg arg) {
        arg.node.handleTransferTimeout(arg.term, arg.peer);
    }

    /**
     * Retrieve current configuration this node seen so far. It's not a reliable way to
     * retrieve cluster peers info, you should use {@link #listPeers()} instead.
     *
     * @return current configuration.
     *
     * @since 1.0.3
     */
    public Configuration getCurrentConf() {
        this.readLock.lock();
        try {
            if (this.conf != null && this.conf.getConf() != null) {
                return this.conf.getConf().copy();
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listPeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return this.conf.getConf().listPeers();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listAlivePeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return getAliveNodes(this.conf.getConf().getPeers(), Utils.monotonicMs());
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void addPeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(!this.conf.getConf().contains(peer), "Peer already exists in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.addPeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void removePeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(this.conf.getConf().contains(peer), "Peer not found in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.removePeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void changePeers(final Configuration newPeers, final Closure done) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        this.writeLock.lock();
        try {
            LOG.info("Node {} change peers from {} to {}.", getNodeId(), this.conf.getConf(), newPeers);
            unsafeRegisterConfChange(this.conf.getConf(), newPeers, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status resetPeers(final Configuration newPeers) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        this.writeLock.lock();
        try {
            if (newPeers.isEmpty()) {
                LOG.warn("Node {} set empty peers.", getNodeId());
                return new Status(RaftError.EINVAL, "newPeers is empty");
            }
            if (!this.state.isActive()) {
                LOG.warn("Node {} is in state {}, can't set peers.", getNodeId(), this.state);
                return new Status(RaftError.EPERM, "Bad state: %s", this.state);
            }
            // bootstrap?
            if (this.conf.getConf().isEmpty()) {
                LOG.info("Node {} set peers to {} from empty.", getNodeId(), newPeers);
                this.conf.setConf(newPeers);
                stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Set peer from empty configuration"));
                return Status.OK();
            }
            if (this.state == State.STATE_LEADER && this.confCtx.isBusy()) {
                LOG.warn("Node {} set peers need wait current conf changing.", getNodeId());
                return new Status(RaftError.EBUSY, "Changing to another configuration");
            }
            // check equal, maybe retry direct return
            if (this.conf.getConf().equals(newPeers)) {
                return Status.OK();
            }
            final Configuration newConf = new Configuration(newPeers);
            LOG.info("Node {} set peers from {} to {}.", getNodeId(), this.conf.getConf(), newPeers);
            this.conf.setConf(newConf);
            this.conf.getOldConf().reset();
            stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Raft node set peer normally"));
            return Status.OK();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 主动生成一次快照 并将结果通知到 closure   如果用户没有主动去创建快照 那么就会通过后台任务来生成快照
     * @param done callback
     */
    @Override
    public void snapshot(final Closure done) {
        doSnapshot(done);
    }

    /**
     * 每个一段时间为当前数据生成快照
     * @param done
     */
    private void doSnapshot(final Closure done) {
        // 首选确保快照选项被开启
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.doSnapshot(done);
        } else {
            if (done != null) {
                final Status status = new Status(RaftError.EINVAL, "Snapshot is not supported");
                Utils.runClosureInThread(done, status);
            }
        }
    }

    @Override
    public void resetElectionTimeoutMs(final int electionTimeoutMs) {
        Requires.requireTrue(electionTimeoutMs > 0, "Invalid electionTimeoutMs");
        this.writeLock.lock();
        try {
            this.options.setElectionTimeoutMs(electionTimeoutMs);
            this.replicatorGroup.resetHeartbeatInterval(heartbeatTimeout(this.options.getElectionTimeoutMs()));
            this.replicatorGroup.resetElectionTimeoutInterval(electionTimeoutMs);
            LOG.info("Node {} reset electionTimeout, currTimer {} state {} new electionTimeout {}.", getNodeId(),
                this.currTerm, this.state, electionTimeoutMs);
            this.electionTimer.reset(electionTimeoutMs);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status transferLeadershipTo(final PeerId peer) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                LOG.warn("Node {} can't transfer leadership to peer {} as it is in state {}.", getNodeId(), peer,
                    this.state);
                return new Status(this.state == State.STATE_TRANSFERRING ? RaftError.EBUSY : RaftError.EPERM,
                        "Not a leader");
            }
            if (this.confCtx.isBusy()) {
                // It's very messy to deal with the case when the |peer| received
                // TimeoutNowRequest and increase the term while somehow another leader
                // which was not replicated with the newest configuration has been
                // elected. If no add_peer with this very |peer| is to be invoked ever
                // after nor this peer is to be killed, this peer will spin in the voting
                // procedure and make the each new leader stepped down when the peer
                // reached vote timeout and it starts to vote (because it will increase
                // the term of the group)
                // To make things simple, refuse the operation and force users to
                // invoke transfer_leadership_to after configuration changing is
                // completed so that the peer's configuration is up-to-date when it
                // receives the TimeOutNowRequest.
                LOG.warn(
                    "Node {} refused to transfer leadership to peer {} when the leader is changing the configuration.",
                    getNodeId(), peer);
                return new Status(RaftError.EBUSY, "Changing the configuration");
            }

            PeerId peerId = peer.copy();
            // if peer_id is ANY_PEER(0.0.0.0:0:0), the peer with the largest
            // last_log_id will be selected.
            if (peerId.equals(PeerId.ANY_PEER)) {
                LOG.info("Node {} starts to transfer leadership to any peer.", getNodeId());
                if ((peerId = this.replicatorGroup.findTheNextCandidate(this.conf)) == null) {
                    return new Status(-1, "Candidate not found for any peer");
                }
            }
            if (peerId.equals(this.serverId)) {
                LOG.info("Node {} transferred leadership to self.", this.serverId);
                return Status.OK();
            }
            if (!this.conf.contains(peerId)) {
                LOG.info("Node {} refused to transfer leadership to peer {} as it is not in {}.", getNodeId(), peer,
                    this.conf);
                return new Status(RaftError.EINVAL, "Not in current configuration");
            }

            final long lastLogIndex = this.logManager.getLastLogIndex();
            if (!this.replicatorGroup.transferLeadershipTo(peerId, lastLogIndex)) {
                LOG.warn("No such peer {}.", peer);
                return new Status(RaftError.EINVAL, "No such peer %s", peer);
            }
            this.state = State.STATE_TRANSFERRING;
            final Status status = new Status(RaftError.ETRANSFERLEADERSHIP,
                "Raft leader is transferring leadership to %s", peerId);
            onLeaderStop(status);
            LOG.info("Node {} starts to transfer leadership to peer {}.", getNodeId(), peer);
            final StopTransferArg stopArg = new StopTransferArg(this, this.currTerm, peerId);
            this.stopTransferArg = stopArg;
            this.transferTimer = this.timerManager.schedule(() -> onTransferTimeout(stopArg),
                this.options.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);

        } finally {
            this.writeLock.unlock();
        }
        return Status.OK();
    }

    private void onLeaderStop(final Status status) {
        this.replicatorGroup.clearFailureReplicators();
        this.fsmCaller.onLeaderStop(status);
    }

    /**
     * 从节点处理 立即超时请求
     * @param request   data of the timeout now request
     * @param done      callback
     * @return
     */
    @Override
    public Message handleTimeoutNowRequest(final TimeoutNowRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (request.getTerm() != this.currTerm) {
                final long savedCurrTerm = this.currTerm;
                // 如果请求的任期更新 很有可能该节点是滞后了
                if (request.getTerm() > this.currTerm) {
                    // 这里主要是 清除leaderId 更新本次term
                    stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMREQUEST,
                        "Raft node receives higher term request"));
                }
                LOG.info("Node {} received TimeoutNowRequest from {} while currTerm={} didn't match requestTerm={}.",
                    getNodeId(), request.getPeerId(), savedCurrTerm, request.getTerm());
                return TimeoutNowResponse.newBuilder() //
                    .setTerm(this.currTerm) // 返回的任期是已经更新过的
                    .setSuccess(false) //
                    .build();
            }
            // 该请求仅针对follower
            if (this.state != State.STATE_FOLLOWER) {
                LOG.info("Node {} received TimeoutNowRequest from {}, while state={}, term={}.", getNodeId(),
                    request.getServerId(), this.state, this.currTerm);
                return TimeoutNowResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setSuccess(false) //
                    .build();
            }

            final long savedTerm = this.currTerm;
            // 代表进入新一轮投票了
            final TimeoutNowResponse resp = TimeoutNowResponse.newBuilder() //
                .setTerm(this.currTerm + 1) //
                .setSuccess(true) //
                .build();
            // Parallelize response and election
            done.sendResponse(resp);
            doUnlock = false;
            // 直接选举自己 跳过预投票阶段 预投票阶段是基于心跳超时的 需要检测其他节点能否正常收到心跳 而直接进入投票阶段 就不需要判断心跳了 只是检测数据是否比半数新 成功的话直接变成leader
            electSelf();
            LOG.info("Node {} received TimeoutNowRequest from {}, term={}.", getNodeId(), request.getServerId(),
                savedTerm);
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        return null;
    }

    /**
     * 处理安装快照的请求
     * @param request   data of the install snapshot request
     * @param done      callback
     * @return
     */
    @Override
    public Message handleInstallSnapshot(final InstallSnapshotRequest request, final RpcRequestClosure done) {
        // 如果本节点没有安装快照执行器 代表不支持快照功能
        if (this.snapshotExecutor == null) {
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Not supported snapshot");
        }
        // 尝试解析对端id
        final PeerId serverId = new PeerId();
        if (!serverId.parse(request.getServerId())) {
            LOG.warn("Node {} ignore InstallSnapshotRequest from {} bad server id.", getNodeId(), request.getServerId());
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse serverId failed: %s", request.getServerId());
        }

        this.writeLock.lock();
        try {
            // 如果当前失活不需要处理
            if (!this.state.isActive()) {
                LOG.warn("Node {} ignore InstallSnapshotRequest as it is not in active state {}.", getNodeId(),
                    this.state);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s:%s is not in active state, state %s.",
                    this.groupId, this.serverId, this.state.name());
            }

            // 代表收到了 旧数据的安装快照请求
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale InstallSnapshotRequest from {}, term={}, currTerm={}.", getNodeId(),
                    request.getPeerId(), request.getTerm(), this.currTerm);
                return InstallSnapshotResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setSuccess(false) //
                    .build();
            }

            checkStepDown(request.getTerm(), serverId);

            // 一般不会进入这种情况
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                    "More than one leader in the same term."));
                return InstallSnapshotResponse.newBuilder() //
                    .setTerm(request.getTerm() + 1) //
                    .setSuccess(false) //
                    .build();
            }

        } finally {
            this.writeLock.unlock();
        }
        final long startMs = Utils.monotonicMs();
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                    "Node {} received InstallSnapshotRequest from {}, lastIncludedLogIndex={}, lastIncludedLogTerm={}, lastLogId={}.",
                    getNodeId(), request.getServerId(), request.getMeta().getLastIncludedIndex(), request.getMeta()
                        .getLastIncludedTerm(), this.logManager.getLastLogId(false));
            }
            // 开始安装快照
            this.snapshotExecutor.installSnapshot(request, InstallSnapshotResponse.newBuilder(), done);
            return null;
        } finally {
            this.metrics.recordLatency("install-snapshot", Utils.monotonicMs() - startMs);
        }
    }

    /**
     * 当安装完本地快照后 更新配置
     */
    public void updateConfigurationAfterInstallingSnapshot() {
        this.writeLock.lock();
        try {
            // 将当前 conf 关联到 ConfigurationManager.ConfigurationEntry 上 ConfigurationEntry 对应到 加载完快照后的信息实体
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void stopReplicator(final List<PeerId> keep, final List<PeerId> drop) {
        if (drop != null) {
            for (final PeerId peer : drop) {
                if (!keep.contains(peer) && !peer.equals(this.serverId)) {
                    this.replicatorGroup.stopReplicator(peer);
                }
            }
        }
    }

    @Override
    public UserLog readCommittedUserLog(final long index) {
        if (index <= 0) {
            throw new LogIndexOutOfBoundsException("Request index is invalid: " + index);
        }

        final long savedLastAppliedIndex = this.fsmCaller.getLastAppliedIndex();

        if (index > savedLastAppliedIndex) {
            throw new LogIndexOutOfBoundsException("Request index " + index + " is greater than lastAppliedIndex: "
                                                   + savedLastAppliedIndex);
        }

        long curIndex = index;
        LogEntry entry = this.logManager.getEntry(curIndex);
        if (entry == null) {
            throw new LogNotFoundException("User log is deleted at index: " + index);
        }

        do {
            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                return new UserLog(curIndex, entry.getData());
            } else {
                curIndex++;
            }
            if (curIndex > savedLastAppliedIndex) {
                throw new IllegalStateException("No user log between index:" + index + " and last_applied_index:"
                                                + savedLastAppliedIndex);
            }
            entry = this.logManager.getEntry(curIndex);
        } while (entry != null);

        throw new LogNotFoundException("User log is deleted at index: " + curIndex);
    }

    @Override
    public void addReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.add(replicatorStateListener);
    }

    @Override
    public void removeReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.remove(replicatorStateListener);
    }

    @Override
    public void clearReplicatorStateListeners() {
        this.replicatorStateListeners.clear();
    }

    @Override
    public List<Replicator.ReplicatorStateListener> getReplicatorStatueListeners() {
        return this.replicatorStateListeners;
    }

    @Override
    public void describe(final Printer out) {
        // node
        final String _nodeId;
        final String _state;
        final long _currTerm;
        final String _conf;
        this.readLock.lock();
        try {
            _nodeId = String.valueOf(getNodeId());
            _state = String.valueOf(this.state);
            _currTerm = this.currTerm;
            _conf = String.valueOf(this.conf);
        } finally {
            this.readLock.unlock();
        }
        out.print("nodeId: ") //
            .println(_nodeId);
        out.print("state: ") //
            .println(_state);
        out.print("term: ") //
            .println(_currTerm);
        out.print("conf: ") //
            .println(_conf);

        // timers
        out.println("electionTimer: ");
        this.electionTimer.describe(out);

        out.println("voteTimer: ");
        this.voteTimer.describe(out);

        out.println("stepDownTimer: ");
        this.stepDownTimer.describe(out);

        out.println("snapshotTimer: ");
        this.snapshotTimer.describe(out);

        // logManager
        out.println("logManager: ");
        this.logManager.describe(out);

        // fsmCaller
        out.println("fsmCaller: ");
        this.fsmCaller.describe(out);

        // ballotBox
        out.println("ballotBox: ");
        this.ballotBox.describe(out);

        // snapshotExecutor
        out.println("snapshotExecutor: ");
        this.snapshotExecutor.describe(out);

        // replicators
        out.println("replicatorGroup: ");
        this.replicatorGroup.describe(out);
    }

    @Override
    public String toString() {
        return "JRaftNode [nodeId=" + getNodeId() + "]";
    }
}
