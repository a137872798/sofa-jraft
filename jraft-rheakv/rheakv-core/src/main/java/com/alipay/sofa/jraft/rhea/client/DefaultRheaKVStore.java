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
package com.alipay.sofa.jraft.rhea.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.FollowerStateListener;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.RegionEngine;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.ListRetryCallable;
import com.alipay.sofa.jraft.rhea.client.failover.RetryCallable;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.client.failover.impl.BoolFailoverFuture;
import com.alipay.sofa.jraft.rhea.client.failover.impl.FailoverClosureImpl;
import com.alipay.sofa.jraft.rhea.client.failover.impl.ListFailoverFuture;
import com.alipay.sofa.jraft.rhea.client.failover.impl.MapFailoverFuture;
import com.alipay.sofa.jraft.rhea.client.pd.FakePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.RemotePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchDeleteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.CompareAndPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteRangeRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetAndPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetSequenceRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyLockRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyUnlockRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.MergeRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.MultiGetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.NodeExecuteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutIfAbsentRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ResetSequenceRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ScanRequest;
import com.alipay.sofa.jraft.rhea.errors.ApiExceptionHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.ErrorsHelper;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetricNames;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVIterator;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.NodeExecutor;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rhea.util.concurrent.AffinityNamedThreadFactory;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.Dispatcher;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.TaskDispatcher;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.WaitStrategyType;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.Histogram;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * Default client of RheaKV store implementation.
 *
 * For example, the processing flow of the method {@link #scan(byte[], byte[])},
 * and the implementation principle of failover:
 * 下面展示了 实现scan 的流程
 *
 * <pre>
 * 1. The first step is to filter out region1, region2, and region3 from the routing table.
 *
 *                ┌─────────────────┐                               ┌─────────────────┐
 *                │  scan startKey  │                               │   scan endKey   │
 *                └────────┬────────┘                               └────────┬────────┘
 *                         │                                                 │
 *                         │                                                 │
 *                         │                                                 │
 * ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─     │  ┌ ─ ─ ─ ─ ─ ─ ┐           ┌ ─ ─ ─ ─ ─ ─ ┐      │    ┌ ─ ─ ─ ─ ─ ─ ┐
 *  startKey1=byte[0] │    │     startKey2                 startKey3         │       startKey4
 * └ ─ ─ ─ ┬ ─ ─ ─ ─ ─     │  └ ─ ─ ─│─ ─ ─ ┘           └ ─ ─ ─│─ ─ ─ ┘      │    └ ─ ─ ─│─ ─ ─ ┘
 *         │               │         │                         │             │           │
 *         ▼───────────────▼─────────▼─────────────────────────▼─────────────▼───────────▼─────────────────────────┐
 *         │                         │                         │                         │                         │
 *         │                         │                         │                         │                         │
 *         │         region1         │         region2         │          region3        │         region4         │
 *         │                         │                         │                         │                         │
 *         └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
 *
 * 2. The second step is to split the request(scan -> multi-region scan):
 *          region1->regionScan(startKey, regionEndKey1)
 *          region2->regionScan(regionStartKey2, regionEndKey2)
 *          region3->regionScan(regionStartKey3, endKey)
 *
 *            ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─      ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                call region1   │        call region2   │         call region3   │
 *            └ ─ ─ ─ ─ ─ ─ ─ ─ ─     └ ─ ─ ─ ─ ─ ─ ─ ─ ─      └ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                     ║                       ║                        ║
 *
 *                     ║                       ║                        ║
 *                     ▽                       ▽                        ▽
 *     ┌─────────────────────────┬─────────────────────────┬─────────────────────────┬─────────────────────────┐
 *     │                         │                         │                         │                         │
 *     │                         │                         │                         │                         │
 *     │         region1         │         region2         │          region3        │         region4         │
 *     │                         │                         │                         │                         │
 *     └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
 *
 * 3. The third step, encountering the region split (the sign of the split is the change of the region epoch)
 *      To refresh the RegionRouteTable, you need to obtain the latest routing table from the PD.
 *
 *      For example, region2 is split into region2 + region5:
 *          The request 'region2->regionScan(regionStartKey2, regionEndKey2)' split and retry
 *              1. region2->regionScan(regionStartKey2, newRegionEndKey2)
 *              2. region5->regionScan(regionStartKey5, regionEndKey5)
 *
 *            ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─                              ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                call region1   │                                 call region3   │
 *            └ ─ ─ ─ ─ ─ ─ ─ ─ ─                              └ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                     ║        ┌ ─ ─ ─ ─ ─ ─ ┐                         ║
 *                               retry region2
 *                     ║        └ ─ ─ ─ ─ ─ ─ ┘┌ ─ ─ ─ ─ ─ ─ ┐          ║
 *                                     ║        retry region5
 *                     ║                       └ ─ ─ ─ ─ ─ ─ ┘          ║
 *                                     ║              ║
 *                     ║                                                ║
 *                     ▽               ▽              ▽                 ▽
 *     ┌─────────────────────────┬────────────┬ ─ ─ ─ ─ ─ ─┌─────────────────────────┬─────────────────────────┐
 *     │                         │            │            │                         │                         │
 *     │                         │            │            │                         │                         │
 *     │         region1         │  region2   │  region5   │          region3        │         region4         │
 *     │                         │            │            │                         │                         │
 *     └─────────────────────────┴────────────┘─ ─ ─ ─ ─ ─ ┴─────────────────────────┴─────────────────────────┘
 *
 * 4. Encountering 'Invalid-Peer'(NOT_LEADER, NO_REGION_FOUND, LEADER_NOT_AVAILABLE)
 *      This is very simple, re-acquire the latest leader of the raft-group to which the current key belongs,
 *      and then call again.
 * </pre>
 * 子模块 内部包含一个 kv关系型存储库 RheaKVStore
 * @author jiachun.fjc
 */
public class DefaultRheaKVStore implements RheaKVStore {

    private static final Logger   LOG = LoggerFactory.getLogger(DefaultRheaKVStore.class);

    static {
        ExtSerializerSupports.init();
    }

    /**
     * 存储引擎
     */
    private StoreEngine           storeEngine;
    /**
     * 管理region 的客户端
     */
    private PlacementDriverClient pdClient;
    /**
     * 发起rpc 请求的服务
     */
    private RheaKVRpcService      rheaKVRpcService;
    private RheaKVStoreOptions    opts;
    private int                   failoverRetries;
    private long                  futureTimeoutMillis;
    private boolean               onlyLeaderRead;
    /**
     * 请求分发器 线程模型是模仿 netty 的
     */
    private Dispatcher            kvDispatcher;
    private BatchingOptions       batchingOpts;
    /**
     * 批量获取
     */
    private GetBatching           getBatching;
    private GetBatching           getBatchingOnlySafe;
    /**
     * 批量添加
     */
    private PutBatching           putBatching;

    private volatile boolean      started;

    /**
     * 初始化KV 存储对象， 该对象基于raft协议实现了 分布式锁
     * @param opts
     * @return
     */
    @Override
    public synchronized boolean init(final RheaKVStoreOptions opts) {
        if (this.started) {
            LOG.info("[DefaultRheaKVStore] already started.");
            return true;
        }
        this.opts = opts;
        // init placement driver  初始化 PD 驱动
        // PD 驱动相当于一个管理中心 所有store的相关信息都可以在上面查询
        final PlacementDriverOptions pdOpts = opts.getPlacementDriverOptions();
        final String clusterName = opts.getClusterName();
        Requires.requireNonNull(pdOpts, "opts.placementDriverOptions");
        Requires.requireNonNull(clusterName, "opts.clusterName");
        if (Strings.isBlank(pdOpts.getInitialServerList())) {
            // if blank, extends parent's value
            // 如果没有初始化服务列表 使用父类的列表
            pdOpts.setInitialServerList(opts.getInitialServerList());
        }
        // 生成 本地 or remote PD驱动客户端
        if (pdOpts.isFake()) {
            this.pdClient = new FakePlacementDriverClient(opts.getClusterId(), clusterName);
        } else {
            this.pdClient = new RemotePlacementDriverClient(opts.getClusterId(), clusterName);
        }
        // 设置PD 中的 region 和 store信息
        if (!this.pdClient.init(pdOpts)) {
            LOG.error("Fail to init [PlacementDriverClient].");
            return false;
        }
        // init store engine
        final StoreEngineOptions stOpts = opts.getStoreEngineOptions();
        if (stOpts != null) {
            stOpts.setInitialServerList(opts.getInitialServerList());
            // 初始化存储引擎 同时会初始化下面所有的 regionEngine 和regionService 还有维护 regionId 与对应服务的映射关系etc
            this.storeEngine = new StoreEngine(this.pdClient);
            if (!this.storeEngine.init(stOpts)) {
                LOG.error("Fail to init [StoreEngine].");
                return false;
            }
        }
        // 获取storeEngine 的 地址信息
        final Endpoint selfEndpoint = this.storeEngine == null ? null : this.storeEngine.getSelfEndpoint();
        final RpcOptions rpcOpts = opts.getRpcOptions();
        Requires.requireNonNull(rpcOpts, "opts.rpcOptions");
        // 创建一个 rpc服务对象 内部封装了 pdClient 用户能直接访问的就是 rpc服务对象
        this.rheaKVRpcService = new DefaultRheaKVRpcService(this.pdClient, selfEndpoint) {

            @Override
            public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
                // 首先尝试从regionEngine中获取leader 如果有就不需要与pd 通信了
                final Endpoint leader = getLeaderByRegionEngine(regionId);
                if (leader != null) {
                    return leader;
                }
                return super.getLeader(regionId, forceRefresh, timeoutMillis);
            }
        };
        // 初始化 kvRpcService  这里为 rpcService 创建了一个线程池
        if (!this.rheaKVRpcService.init(rpcOpts)) {
            LOG.error("Fail to init [RheaKVRpcService].");
            return false;
        }
        this.failoverRetries = opts.getFailoverRetries();
        this.futureTimeoutMillis = opts.getFutureTimeoutMillis();
        this.onlyLeaderRead = opts.isOnlyLeaderRead();
        // 如果使用并行执行器
        if (opts.isUseParallelKVExecutor()) {
            final int numWorkers = Utils.cpus();
            final int bufSize = numWorkers << 4;
            final String name = "parallel-kv-executor";
            // 根据情况生成不同的 线程工厂  这里生成的是守护线程
            final ThreadFactory threadFactory = Constants.THREAD_AFFINITY_ENABLED
                    ? new AffinityNamedThreadFactory(name, true) : new NamedThreadFactory(name, true);
            // 使用线程工厂构建任务选择器 内部利用了 disruptorr
            this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
        }
        this.batchingOpts = opts.getBatchingOptions();
        // 初始化批量对象
        if (this.batchingOpts.isAllowBatching()) {
            this.getBatching = new GetBatching(KeyEvent::new, "get_batching",
                    new GetBatchingHandler("get", false));
            this.getBatchingOnlySafe = new GetBatching(KeyEvent::new, "get_batching_only_safe",
                    new GetBatchingHandler("get_only_safe", true));
            this.putBatching = new PutBatching(KVEvent::new, "put_batching",
                    new PutBatchingHandler("put"));
        }
        LOG.info("[DefaultRheaKVStore] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        this.started = false;
        if (this.pdClient != null) {
            this.pdClient.shutdown();
        }
        if (this.storeEngine != null) {
            this.storeEngine.shutdown();
        }
        if (this.rheaKVRpcService != null) {
            this.rheaKVRpcService.shutdown();
        }
        if (this.kvDispatcher != null) {
            this.kvDispatcher.shutdown();
        }
        if (this.getBatching != null) {
            this.getBatching.shutdown();
        }
        if (this.getBatchingOnlySafe != null) {
            this.getBatchingOnlySafe.shutdown();
        }
        if (this.putBatching != null) {
            this.putBatching.shutdown();
        }
        LOG.info("[DefaultRheaKVStore] shutdown successfully.");
    }

    /**
     * Returns a heap-allocated iterator over the contents of the
     * database.
     * <p>
     * Caller should close the iterator when it is no longer needed.
     * The returned iterator should be closed before this db is closed.
     * <p>
     * <pre>
     *     KVIterator it = unsafeLocalIterator();
     *     try {
     *         // do something
     *     } finally {
     *         it.close();
     *     }
     * <pre/>
     * 将数据 包装成一个迭代器
     */
    public KVIterator unsafeLocalIterator() {
        // 确保当前服务器在启动状态
        checkState();
        if (this.pdClient instanceof RemotePlacementDriverClient) {
            throw new UnsupportedOperationException("unsupported operation on multi-region");
        }
        if (this.storeEngine == null) {
            throw new IllegalStateException("current node do not have store engine");
        }
        return this.storeEngine.getRawKVStore().localIterator();
    }

    @Override
    public CompletableFuture<byte[]> get(final byte[] key) {
        return get(key, true);
    }

    @Override
    public CompletableFuture<byte[]> get(final String key) {
        return get(BytesUtil.writeUtf8(key));
    }

    @Override
    public CompletableFuture<byte[]> get(final byte[] key, final boolean readOnlySafe) {
        Requires.requireNonNull(key, "key");
        return get(key, readOnlySafe, new CompletableFuture<>(), true);
    }

    @Override
    public CompletableFuture<byte[]> get(final String key, final boolean readOnlySafe) {
        return get(BytesUtil.writeUtf8(key), readOnlySafe);
    }

    /**
     * blockget 阻塞获取
     * @param key
     * @return
     */
    @Override
    public byte[] bGet(final byte[] key) {
        return FutureHelper.get(get(key), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final String key) {
        return FutureHelper.get(get(key), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final byte[] key, final boolean readOnlySafe) {
        return FutureHelper.get(get(key, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final String key, final boolean readOnlySafe) {
        return FutureHelper.get(get(key, readOnlySafe), this.futureTimeoutMillis);
    }

    /**
     * 尝试获取某个 key 对应的数据 将结果设置到 future 中并返回
     * @param key
     * @param readOnlySafe 默认为true
     * @param future
     * @param tryBatching  默认为true
     * @return
     */
    private CompletableFuture<byte[]> get(final byte[] key, final boolean readOnlySafe,
                                          final CompletableFuture<byte[]> future, final boolean tryBatching) {
        checkState();
        Requires.requireNonNull(key, "key");
        if (tryBatching) {
            // 发布一个 拉取数据的任务到 disruptor 中
            final GetBatching getBatching = readOnlySafe ? this.getBatchingOnlySafe : this.getBatching;
            if (getBatching != null && getBatching.apply(key, future)) {
                return future;
            }
        }
        // 如果不是批量任务
        internalGet(key, readOnlySafe, future, this.failoverRetries, null, this.onlyLeaderRead);
        return future;
    }

    /**
     * 非批量任务  通过key 查找数据落在哪个region  每个region 通过 startKey 和 endKey 来划分
     * @param key
     * @param readOnlySafe
     * @param future
     * @param retriesLeft
     * @param lastCause
     * @param requireLeader
     */
    private void internalGet(final byte[] key, final boolean readOnlySafe, final CompletableFuture<byte[]> future,
                             final int retriesLeft, final Errors lastCause, final boolean requireLeader) {
        // pdClient 内部有2个map 一个维护了 regionId 与 region的关系 还有一个维护了 key 与 regionId的关系
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        // require 必须确保regionEngine 对应的node 是 leader  在内部的table 有维护region 与regionEngine 的关系
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry  这里定义了重试的逻辑 失败则 使用 requireLeader == true 再执行一次
        final RetryRunner retryRunner = retryCause -> internalGet(key, readOnlySafe, future, retriesLeft - 1,
                retryCause, true);
        // 将结果填充到future 中
        final FailoverClosure<byte[]> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        // 当成功查询到 地区引擎时
        if (regionEngine != null) {
            // 这里是在比较 region 和 regionEngine 的 regionEpoch 是否相同
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                // 从存储层拉取数据 并触发回调 这里会将 结果设置到future中如果失败 就会重新调用internalGet
                getRawKVStore(regionEngine).get(key, readOnlySafe, closure);
            }
        // 没有查询到就需要通过远程调用 获取某个服务器上的信息
        } else {
            final GetRequest request = new GetRequest();
            request.setKey(key);
            request.setReadOnlySafe(readOnlySafe);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }

    @Override
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys) {
        return multiGet(keys, true);
    }

    @Override
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys, final boolean readOnlySafe) {
        checkState();
        Requires.requireNonNull(keys, "keys");
        final FutureGroup<Map<ByteArray, byte[]>> futureGroup = internalMultiGet(keys, readOnlySafe,
            this.failoverRetries, null);
        return FutureHelper.joinMap(futureGroup, keys.size());
    }

    @Override
    public Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys) {
        return FutureHelper.get(multiGet(keys), this.futureTimeoutMillis);
    }

    @Override
    public Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys, final boolean readOnlySafe) {
        return FutureHelper.get(multiGet(keys, readOnlySafe), this.futureTimeoutMillis);
    }

    /**
     * 拉取多个数据
     * @param keys
     * @param readOnlySafe
     * @param retriesLeft
     * @param lastCause
     * @return
     */
    private FutureGroup<Map<ByteArray, byte[]>> internalMultiGet(final List<byte[]> keys, final boolean readOnlySafe,
                                                                 final int retriesLeft, final Throwable lastCause) {
        // 从 映射表中找到 key 对应的region 并返回 多个key 可能会对应同一个region
        final Map<Region, List<byte[]>> regionMap = this.pdClient
                .findRegionsByKeys(keys, ApiExceptionHelper.isInvalidEpoch(lastCause));
        final List<CompletableFuture<Map<ByteArray, byte[]>>> futures = Lists.newArrayListWithCapacity(regionMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<Region, List<byte[]>> entry : regionMap.entrySet()) {
            final Region region = entry.getKey();
            final List<byte[]> subKeys = entry.getValue();
            // 生成 重试的回调
            final RetryCallable<Map<ByteArray, byte[]>> retryCallable = retryCause -> internalMultiGet(subKeys,
                    readOnlySafe, retriesLeft - 1, retryCause);
            final MapFailoverFuture<ByteArray, byte[]> future = new MapFailoverFuture<>(retriesLeft, retryCallable);
            // 执行批量拉取任务 并返回相关的future 客户只需要 调用get 等待数据被设置就可以
            internalRegionMultiGet(region, subKeys, readOnlySafe, future, retriesLeft, lastError, this.onlyLeaderRead);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    /**
     * 批量get 数据
     * @param region
     * @param subKeys
     * @param readOnlySafe
     * @param future
     * @param retriesLeft
     * @param lastCause
     * @param requireLeader
     */
    private void internalRegionMultiGet(final Region region, final List<byte[]> subKeys, final boolean readOnlySafe,
                                        final CompletableFuture<Map<ByteArray, byte[]>> future, final int retriesLeft,
                                        final Errors lastCause, final boolean requireLeader) {
        // 这个引擎到底是干嘛的
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry
        final RetryRunner retryRunner = retryCause -> internalRegionMultiGet(region, subKeys, readOnlySafe, future,
                retriesLeft - 1, retryCause, true);
        final FailoverClosure<Map<ByteArray, byte[]>> closure = new FailoverClosureImpl<>(future,
                false, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                // 获取统计对象 忽略
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.multiGet(subKeys, readOnlySafe, closure);
                } else {
                    // 使用disruptor 执行任务  批量拉取数据并保存到future 中
                    this.kvDispatcher.execute(() -> rawKVStore.multiGet(subKeys, readOnlySafe, closure));
                }
            }
        } else {
            final MultiGetRequest request = new MultiGetRequest();
            request.setKeys(subKeys);
            request.setReadOnlySafe(readOnlySafe);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            // 否则使用rpc  调用 为什么要分成2种获取方式 ???  哦 可能 store 意味着在本机的某个地方持久化了数据 如果没有找到持久化对象
            // 那么就使用rpc 框架拉取远端数据
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }

    // 下面的方法都差不多 实际上本对象作为一个入口 最后会调用store 的对应api

    @Override
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey) {
        return scan(startKey, endKey, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey) {
        return scan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey));
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe) {
        return scan(startKey, endKey, readOnlySafe, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey, final boolean readOnlySafe) {
        return scan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey,
                                                 final boolean readOnlySafe, final boolean returnValue) {
        checkState();
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
        if (endKey != null) {
            Requires.requireTrue(BytesUtil.compare(realStartKey, endKey) < 0, "startKey must < endKey");
        }
        final FutureGroup<List<KVEntry>> futureGroup = internalScan(realStartKey, endKey, readOnlySafe, returnValue,
            this.failoverRetries, null);
        return FutureHelper.joinList(futureGroup);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey,
                                                 final boolean readOnlySafe, final boolean returnValue) {
        return scan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe, returnValue);
    }

    @Override
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey) {
        return FutureHelper.get(scan(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final String startKey, final String endKey) {
        return FutureHelper.get(scan(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                               final boolean returnValue) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe,
                               final boolean returnValue) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    /**
     * 针对 跨越多个 region 的 scan 会先找到 startKey endKey 包含的region 区间 之后为每个region 分开调用scan 并将多个 Completable 合并成FutureGroup
     * @param startKey
     * @param endKey
     * @param readOnlySafe
     * @param returnValue
     * @param retriesLeft
     * @param lastCause
     * @return
     */
    private FutureGroup<List<KVEntry>> internalScan(final byte[] startKey, final byte[] endKey,
                                                    final boolean readOnlySafe, final boolean returnValue,
                                                    final int retriesLeft, final Throwable lastCause) {
        Requires.requireNonNull(startKey, "startKey");
        final List<Region> regionList = this.pdClient
                .findRegionsByKeyRange(startKey, endKey, ApiExceptionHelper.isInvalidEpoch(lastCause));
        final List<CompletableFuture<List<KVEntry>>> futures = Lists.newArrayListWithCapacity(regionList.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Region region : regionList) {
            final byte[] regionStartKey = region.getStartKey();
            final byte[] regionEndKey = region.getEndKey();
            final byte[] subStartKey = regionStartKey == null ? startKey : BytesUtil.max(regionStartKey, startKey);
            final byte[] subEndKey = regionEndKey == null ? endKey :
                    (endKey == null ? regionEndKey : BytesUtil.min(regionEndKey, endKey));
            final ListRetryCallable<KVEntry> retryCallable = retryCause -> internalScan(subStartKey, subEndKey,
                    readOnlySafe, returnValue, retriesLeft - 1, retryCause);
            final ListFailoverFuture<KVEntry> future = new ListFailoverFuture<>(retriesLeft, retryCallable);
            internalRegionScan(region, subStartKey, subEndKey, readOnlySafe, returnValue, future, retriesLeft, lastError,
                    this.onlyLeaderRead);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    /**
     * 扫描某个区域的数据   region epoch 代表着该region 是否发生分裂  如果发生分裂要重新拉取 数据 并进一步 分解 请求
     * @param region
     * @param subStartKey
     * @param subEndKey
     * @param readOnlySafe
     * @param returnValue
     * @param future
     * @param retriesLeft
     * @param lastCause
     * @param requireLeader
     */
    private void internalRegionScan(final Region region, final byte[] subStartKey, final byte[] subEndKey,
                                    final boolean readOnlySafe, final boolean returnValue,
                                    final CompletableFuture<List<KVEntry>> future, final int retriesLeft,
                                    final Errors lastCause, final boolean requireLeader) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry
        final RetryRunner retryRunner = retryCause -> internalRegionScan(region, subStartKey, subEndKey, readOnlySafe,
                returnValue, future, retriesLeft - 1, retryCause, true);
        final FailoverClosure<List<KVEntry>> closure = new FailoverClosureImpl<>(future, false,
                retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.scan(subStartKey, subEndKey, readOnlySafe, returnValue, closure);
                } else {
                    this.kvDispatcher.execute(
                            () -> rawKVStore.scan(subStartKey, subEndKey, readOnlySafe, returnValue, closure));
                }
            }
        } else {
            final ScanRequest request = new ScanRequest();
            request.setStartKey(subStartKey);
            request.setEndKey(subEndKey);
            request.setReadOnlySafe(readOnlySafe);
            request.setReturnValue(returnValue);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }

    public List<KVEntry> singleRegionScan(final byte[] startKey, final byte[] endKey, final int limit,
                                          final boolean readOnlySafe, final boolean returnValue) {
        checkState();
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
        if (endKey != null) {
            Requires.requireTrue(BytesUtil.compare(realStartKey, endKey) < 0, "startKey must < endKey");
        }
        Requires.requireTrue(limit > 0, "limit must > 0");
        final CompletableFuture<List<KVEntry>> future = new CompletableFuture<>();
        internalSingleRegionScan(realStartKey, endKey, limit, readOnlySafe, returnValue, future, this.failoverRetries,
            null, this.onlyLeaderRead);
        return FutureHelper.get(future, this.futureTimeoutMillis);
    }

    private void internalSingleRegionScan(final byte[] startKey, final byte[] endKey, final int limit,
                                          final boolean readOnlySafe, final boolean returnValue,
                                          final CompletableFuture<List<KVEntry>> future, final int retriesLeft,
                                          final Errors lastCause, final boolean requireLeader) {
        Requires.requireNonNull(startKey, "startKey");
        final Region region = this.pdClient.findRegionByKey(startKey, ErrorsHelper.isInvalidEpoch(lastCause));
        final byte[] regionEndKey = region.getEndKey();
        final byte[] realEndKey = regionEndKey == null ? endKey :
                (endKey == null ? regionEndKey : BytesUtil.min(regionEndKey, endKey));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry
        final RetryRunner retryRunner = retryCause -> internalSingleRegionScan(startKey, endKey, limit, readOnlySafe,
                returnValue, future, retriesLeft - 1, retryCause, true);
        final FailoverClosure<List<KVEntry>> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).scan(startKey, realEndKey, limit, readOnlySafe, returnValue, closure);
            }
        } else {
            final ScanRequest request = new ScanRequest();
            request.setStartKey(startKey);
            request.setEndKey(realEndKey);
            request.setLimit(limit);
            request.setReadOnlySafe(readOnlySafe);
            request.setReturnValue(returnValue);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }

    @Override
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize) {
        return iterator(startKey, endKey, bufSize, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize) {
        return iterator(startKey, endKey, bufSize, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                          final boolean readOnlySafe) {
        return iterator(startKey, endKey, bufSize, readOnlySafe, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                          final boolean readOnlySafe) {
        return iterator(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), bufSize, readOnlySafe);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                          final boolean readOnlySafe, final boolean returnValue) {
        return new DefaultRheaIterator(this, startKey, endKey, bufSize, readOnlySafe, returnValue);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                          final boolean readOnlySafe, final boolean returnValue) {
        return iterator(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), bufSize, readOnlySafe, returnValue);
    }

    @Override
    public CompletableFuture<Sequence> getSequence(final byte[] seqKey, final int step) {
        checkState();
        Requires.requireNonNull(seqKey, "seqKey");
        Requires.requireTrue(step >= 0, "step must >= 0");
        final CompletableFuture<Sequence> future = new CompletableFuture<>();
        internalGetSequence(seqKey, step, future, this.failoverRetries, null);
        return future;
    }

    @Override
    public CompletableFuture<Sequence> getSequence(final String seqKey, final int step) {
        return getSequence(BytesUtil.writeUtf8(seqKey), step);
    }

    @Override
    public Sequence bGetSequence(final byte[] seqKey, final int step) {
        return FutureHelper.get(getSequence(seqKey, step), this.futureTimeoutMillis);
    }

    @Override
    public Sequence bGetSequence(final String seqKey, final int step) {
        return FutureHelper.get(getSequence(seqKey, step), this.futureTimeoutMillis);
    }

    @Override
    public CompletableFuture<Long> getLatestSequence(final byte[] seqKey) {
        final CompletableFuture<Long> cf = new CompletableFuture<>();
        getSequence(seqKey, 0).whenComplete((sequence, throwable) -> {
            if (throwable == null) {
                cf.complete(sequence.getStartValue());
            } else {
                cf.completeExceptionally(throwable);
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Long> getLatestSequence(final String seqKey) {
        return getLatestSequence(BytesUtil.writeUtf8(seqKey));
    }

    @Override
    public Long bGetLatestSequence(final byte[] seqKey) {
        return FutureHelper.get(getLatestSequence(seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Long bGetLatestSequence(final String seqKey) {
        return FutureHelper.get(getLatestSequence(seqKey), this.futureTimeoutMillis);
    }

    private void internalGetSequence(final byte[] seqKey, final int step, final CompletableFuture<Sequence> future,
                                     final int retriesLeft, final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(seqKey, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalGetSequence(seqKey, step, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Sequence> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).getSequence(seqKey, step, closure);
            }
        } else {
            final GetSequenceRequest request = new GetSequenceRequest();
            request.setSeqKey(seqKey);
            request.setStep(step);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> resetSequence(final byte[] seqKey) {
        checkState();
        Requires.requireNonNull(seqKey, "seqKey");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalResetSequence(seqKey, future, this.failoverRetries, null);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> resetSequence(final String seqKey) {
        return resetSequence(BytesUtil.writeUtf8(seqKey));
    }

    @Override
    public Boolean bResetSequence(final byte[] seqKey) {
        return FutureHelper.get(resetSequence(seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bResetSequence(final String seqKey) {
        return FutureHelper.get(resetSequence(seqKey), this.futureTimeoutMillis);
    }

    private void internalResetSequence(final byte[] seqKey, final CompletableFuture<Boolean> future,
                                       final int retriesLeft, final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(seqKey, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalResetSequence(seqKey, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).resetSequence(seqKey, closure);
            }
        } else {
            final ResetSequenceRequest request = new ResetSequenceRequest();
            request.setSeqKey(seqKey);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> put(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return put(key, value, new CompletableFuture<>(), true);
    }

    @Override
    public CompletableFuture<Boolean> put(final String key, final byte[] value) {
        return put(BytesUtil.writeUtf8(key), value);
    }

    @Override
    public Boolean bPut(final byte[] key, final byte[] value) {
        return FutureHelper.get(put(key, value), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bPut(final String key, final byte[] value) {
        return FutureHelper.get(put(key, value), this.futureTimeoutMillis);
    }

    private CompletableFuture<Boolean> put(final byte[] key, final byte[] value,
                                           final CompletableFuture<Boolean> future, final boolean tryBatching) {
        checkState();
        if (tryBatching) {
            final PutBatching putBatching = this.putBatching;
            if (putBatching != null && putBatching.apply(new KVEntry(key, value), future)) {
                return future;
            }
        }
        internalPut(key, value, future, this.failoverRetries, null);
        return future;
    }

    private void internalPut(final byte[] key, final byte[] value, final CompletableFuture<Boolean> future,
                             final int retriesLeft, final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalPut(key, value, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).put(key, value, closure);
            }
        } else {
            final PutRequest request = new PutRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<byte[]> getAndPut(final byte[] key, final byte[] value) {
        checkState();
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        internalGetAndPut(key, value, future, this.failoverRetries, null);
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getAndPut(final String key, final byte[] value) {
        return getAndPut(BytesUtil.writeUtf8(key), value);
    }

    @Override
    public byte[] bGetAndPut(final byte[] key, final byte[] value) {
        return FutureHelper.get(getAndPut(key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGetAndPut(final String key, final byte[] value) {
        return FutureHelper.get(getAndPut(key, value), this.futureTimeoutMillis);
    }

    private void internalGetAndPut(final byte[] key, final byte[] value, final CompletableFuture<byte[]> future,
                                   final int retriesLeft, final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalGetAndPut(key, value, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<byte[]> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).getAndPut(key, value, closure);
            }
        } else {
            final GetAndPutRequest request = new GetAndPutRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> compareAndPut(final byte[] key, final byte[] expect, final byte[] update) {
        checkState();
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(expect, "expect");
        Requires.requireNonNull(update, "update");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalCompareAndPut(key, expect, update, future, this.failoverRetries, null);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> compareAndPut(final String key, final byte[] expect, final byte[] update) {
        return compareAndPut(BytesUtil.writeUtf8(key), expect, update);
    }

    @Override
    public Boolean bCompareAndPut(final byte[] key, final byte[] expect, final byte[] update) {
        return FutureHelper.get(compareAndPut(key, expect, update), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bCompareAndPut(final String key, final byte[] expect, final byte[] update) {
        return FutureHelper.get(compareAndPut(key, expect, update), this.futureTimeoutMillis);
    }

    private void internalCompareAndPut(final byte[] key, final byte[] expect, final byte[] update,
                                       final CompletableFuture<Boolean> future, final int retriesLeft,
                                       final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalCompareAndPut(key, expect, update, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                // 当往node成功写入数据后 会触发回调，执行真正的写入操作
                getRawKVStore(regionEngine).compareAndPut(key, expect, update, closure);
            }
        } else {
            final CompareAndPutRequest request = new CompareAndPutRequest();
            request.setKey(key);
            request.setExpect(expect);
            request.setUpdate(update);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> merge(final String key, final String value) {
        checkState();
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalMerge(BytesUtil.writeUtf8(key), BytesUtil.writeUtf8(value), future, this.failoverRetries, null);
        return future;
    }

    @Override
    public Boolean bMerge(final String key, final String value) {
        return FutureHelper.get(merge(key, value), this.futureTimeoutMillis);
    }

    private void internalMerge(final byte[] key, final byte[] value, final CompletableFuture<Boolean> future,
                               final int retriesLeft, final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalMerge(key, value, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).merge(key, value, closure);
            }
        } else {
            final MergeRequest request = new MergeRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    // Note: the current implementation, if the 'keys' are distributed across
    // multiple regions, can not provide transaction guarantee.
    @Override
    public CompletableFuture<Boolean> put(final List<KVEntry> entries) {
        checkState();
        Requires.requireNonNull(entries, "entries");
        Requires.requireTrue(!entries.isEmpty(), "entries empty");
        final FutureGroup<Boolean> futureGroup = internalPut(entries, this.failoverRetries, null);
        return FutureHelper.joinBooleans(futureGroup);
    }

    @Override
    public Boolean bPut(final List<KVEntry> entries) {
        return FutureHelper.get(put(entries), this.futureTimeoutMillis);
    }

    private FutureGroup<Boolean> internalPut(final List<KVEntry> entries, final int retriesLeft,
                                             final Throwable lastCause) {
        final Map<Region, List<KVEntry>> regionMap = this.pdClient
                .findRegionsByKvEntries(entries, ApiExceptionHelper.isInvalidEpoch(lastCause));
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<Region, List<KVEntry>> entry : regionMap.entrySet()) {
            final Region region = entry.getKey();
            final List<KVEntry> subEntries = entry.getValue();
            final RetryCallable<Boolean> retryCallable = retryCause -> internalPut(subEntries, retriesLeft - 1,
                    retryCause);
            final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            internalRegionPut(region, subEntries, future, retriesLeft, lastError);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    private void internalRegionPut(final Region region, final List<KVEntry> subEntries,
                                   final CompletableFuture<Boolean> future, final int retriesLeft,
                                   final Errors lastCause) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionPut(region, subEntries, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.put(subEntries, closure);
                } else {
                    this.kvDispatcher.execute(() -> rawKVStore.put(subEntries, closure));
                }
            }
        } else {
            final BatchPutRequest request = new BatchPutRequest();
            request.setKvEntries(subEntries);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    /**
     * 存储某个数据
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return
     */
    @Override
    public CompletableFuture<byte[]> putIfAbsent(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        internalPutIfAbsent(key, value, future, this.failoverRetries, null);
        return future;
    }

    @Override
    public CompletableFuture<byte[]> putIfAbsent(final String key, final byte[] value) {
        return putIfAbsent(BytesUtil.writeUtf8(key), value);
    }

    @Override
    public byte[] bPutIfAbsent(final byte[] key, final byte[] value) {
        return FutureHelper.get(putIfAbsent(key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bPutIfAbsent(final String key, final byte[] value) {
        return FutureHelper.get(putIfAbsent(key, value), this.futureTimeoutMillis);
    }

    private void internalPutIfAbsent(final byte[] key, final byte[] value, final CompletableFuture<byte[]> future,
                                     final int retriesLeft, final Errors lastCause) {
        // 找到 该key 对应的region
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalPutIfAbsent(key, value, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<byte[]> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).putIfAbsent(key, value, closure);
            }
        } else {
            final PutIfAbsentRequest request = new PutIfAbsentRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> delete(final byte[] key) {
        checkState();
        Requires.requireNonNull(key, "key");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalDelete(key, future, this.failoverRetries, null);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> delete(final String key) {
        return delete(BytesUtil.writeUtf8(key));
    }

    @Override
    public Boolean bDelete(final byte[] key) {
        return FutureHelper.get(delete(key), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDelete(final String key) {
        return FutureHelper.get(delete(key), this.futureTimeoutMillis);
    }

    private void internalDelete(final byte[] key, final CompletableFuture<Boolean> future, final int retriesLeft,
                                final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalDelete(key, future, retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).delete(key, closure);
            }
        } else {
            final DeleteRequest request = new DeleteRequest();
            request.setKey(key);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> deleteRange(final byte[] startKey, final byte[] endKey) {
        checkState();
        Requires.requireNonNull(startKey, "startKey");
        Requires.requireNonNull(endKey, "endKey");
        Requires.requireTrue(BytesUtil.compare(startKey, endKey) < 0, "startKey must < endKey");
        final FutureGroup<Boolean> futureGroup = internalDeleteRange(startKey, endKey, this.failoverRetries, null);
        return FutureHelper.joinBooleans(futureGroup);
    }

    private FutureGroup<Boolean> internalDeleteRange(final byte[] startKey, final byte[] endKey, final int retriesLeft,
                                                     final Throwable lastCause) {
        final List<Region> regionList = this.pdClient
                .findRegionsByKeyRange(startKey, endKey, ApiExceptionHelper.isInvalidEpoch(lastCause));
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionList.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Region region : regionList) {
            final byte[] regionStartKey = region.getStartKey();
            final byte[] regionEndKey = region.getEndKey();
            final byte[] subStartKey = regionStartKey == null ? startKey : BytesUtil.max(regionStartKey, startKey);
            final byte[] subEndKey = regionEndKey == null ? endKey : BytesUtil.min(regionEndKey, endKey);
            final RetryCallable<Boolean> retryCallable = retryCause -> internalDeleteRange(subStartKey, subEndKey,
                    retriesLeft - 1, retryCause);
            final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            internalRegionDeleteRange(region, subStartKey, subEndKey, future, retriesLeft, lastError);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    @Override
    public CompletableFuture<Boolean> deleteRange(final String startKey, final String endKey) {
        return deleteRange(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey));
    }

    @Override
    public Boolean bDeleteRange(final byte[] startKey, final byte[] endKey) {
        return FutureHelper.get(deleteRange(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDeleteRange(final String startKey, final String endKey) {
        return FutureHelper.get(deleteRange(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public CompletableFuture<Boolean> delete(final List<byte[]> keys) {
        checkState();
        Requires.requireNonNull(keys, "keys");
        Requires.requireTrue(!keys.isEmpty(), "keys empty");
        final FutureGroup<Boolean> futureGroup = internalDelete(keys, this.failoverRetries, null);
        return FutureHelper.joinBooleans(futureGroup);
    }

    @Override
    public Boolean bDelete(final List<byte[]> keys) {
        return FutureHelper.get(delete(keys), this.futureTimeoutMillis);
    }

    private FutureGroup<Boolean> internalDelete(final List<byte[]> keys, final int retriesLeft,
                                                final Throwable lastCause) {
        final Map<Region, List<byte[]>> regionMap = this.pdClient
                .findRegionsByKeys(keys, ApiExceptionHelper.isInvalidEpoch(lastCause));
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<Region, List<byte[]>> entry : regionMap.entrySet()) {
            final Region region = entry.getKey();
            final List<byte[]> subKeys = entry.getValue();
            final RetryCallable<Boolean> retryCallable = retryCause -> internalDelete(subKeys, retriesLeft - 1,
                    retryCause);
            final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            internalRegionDelete(region, subKeys, future, retriesLeft, lastError);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    private void internalRegionDelete(final Region region, final List<byte[]> subKeys,
                                      final CompletableFuture<Boolean> future, final int retriesLeft,
                                      final Errors lastCause) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionDelete(region, subKeys, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.delete(subKeys, closure);
                } else {
                    this.kvDispatcher.execute(() -> rawKVStore.delete(subKeys, closure));
                }
            }
        } else {
            final BatchDeleteRequest request = new BatchDeleteRequest();
            request.setKeys(subKeys);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    private void internalRegionDeleteRange(final Region region, final byte[] subStartKey, final byte[] subEndKey,
                                           final CompletableFuture<Boolean> future, final int retriesLeft,
                                           final Errors lastCause) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionDeleteRange(region, subStartKey, subEndKey, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure =
                new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).deleteRange(subStartKey, subEndKey, closure);
            }
        } else {
            final DeleteRangeRequest request = new DeleteRangeRequest();
            request.setStartKey(subStartKey);
            request.setEndKey(subEndKey);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    // internal api  传入一个 executor 本次执行结果 会同步到raft 所有节点  由用户实现
    public CompletableFuture<Boolean> execute(final long regionId, final NodeExecutor executor) {
        checkState();
        Requires.requireNonNull(executor, "executor");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalExecute(regionId, executor, future, this.failoverRetries, null);
        return future;
    }

    // internal api
    public Boolean bExecute(final long regionId, final NodeExecutor executor) {
        return FutureHelper.get(execute(regionId, executor), this.futureTimeoutMillis);
    }

    private void internalExecute(final long regionId, final NodeExecutor executor,
                                 final CompletableFuture<Boolean> future, final int retriesLeft, final Errors lastCause) {
        final Region region = this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalExecute(regionId, executor, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).execute(executor, true, closure);
            }
        } else {
            final NodeExecuteRequest request = new NodeExecuteRequest();
            request.setNodeExecutor(executor);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit) {
        return getDistributedLock(target, lease, unit, null);
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit) {
        return getDistributedLock(target, lease, unit, null);
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit,
                                                      final ScheduledExecutorService watchdog) {
        return new DefaultDistributedLock(target, lease, unit, watchdog, this);
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit,
                                                      final ScheduledExecutorService watchdog) {
        return getDistributedLock(BytesUtil.writeUtf8(target), lease, unit, watchdog);
    }

    public CompletableFuture<DistributedLock.Owner> tryLockWith(final byte[] key, final boolean keepLease,
                                                                final DistributedLock.Acquirer acquirer) {
        checkState();
        Requires.requireNonNull(key, "key");
        final CompletableFuture<DistributedLock.Owner> future = new CompletableFuture<>();
        internalTryLockWith(key, keepLease, acquirer, future, this.failoverRetries, null);
        return future;
    }

    private void internalTryLockWith(final byte[] key, final boolean keepLease, final DistributedLock.Acquirer acquirer,
                                     final CompletableFuture<DistributedLock.Owner> future, final int retriesLeft,
                                     final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalTryLockWith(key, keepLease, acquirer, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<DistributedLock.Owner> closure = new FailoverClosureImpl<>(future, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).tryLockWith(key, region.getStartKey(), keepLease, acquirer, closure);
            }
        } else {
            final KeyLockRequest request = new KeyLockRequest();
            request.setKey(key);
            request.setKeepLease(keepLease);
            request.setAcquirer(acquirer);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    /**
     * 通过 key 和 acq 对象释放锁
     * @param key
     * @param acquirer
     * @return
     */
    public CompletableFuture<DistributedLock.Owner> releaseLockWith(final byte[] key,
                                                                    final DistributedLock.Acquirer acquirer) {
        checkState();
        Requires.requireNonNull(key, "key");
        final CompletableFuture<DistributedLock.Owner> future = new CompletableFuture<>();
        internalReleaseLockWith(key, acquirer, future, this.failoverRetries, null);
        return future;
    }

    private void internalReleaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer,
                                         final CompletableFuture<DistributedLock.Owner> future, final int retriesLeft,
                                         final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalReleaseLockWith(key, acquirer, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<DistributedLock.Owner> closure = new FailoverClosureImpl<>(future, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).releaseLockWith(key, acquirer, closure);
            }
        } else {
            final KeyUnlockRequest request = new KeyUnlockRequest();
            request.setKey(key);
            request.setAcquirer(acquirer);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public PlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    @Override
    public void addLeaderStateListener(final long regionId, final LeaderStateListener listener) {
        addStateListener(regionId, listener);
    }

    @Override
    public void addFollowerStateListener(final long regionId, final FollowerStateListener listener) {
        addStateListener(regionId, listener);
    }

    @Override
    public void addStateListener(final long regionId, final StateListener listener) {
        checkState();
        if (this.storeEngine == null) {
            throw new IllegalStateException("current node do not have store engine");
        }
        final RegionEngine regionEngine = this.storeEngine.getRegionEngine(regionId);
        if (regionEngine == null) {
            throw new IllegalStateException("current node do not have this region engine[" + regionId + "]");
        }
        regionEngine.getFsm().addStateListener(listener);
    }

    public long getClusterId() {
        return this.opts.getClusterId();
    }

    public StoreEngine getStoreEngine() {
        return storeEngine;
    }

    public boolean isOnlyLeaderRead() {
        return onlyLeaderRead;
    }

    public boolean isLeader(final long regionId) {
        checkState();
        final RegionEngine regionEngine = getRegionEngine(regionId);
        return regionEngine != null && regionEngine.isLeader();
    }

    private void checkState() {
        // Not a strict state check, more is to use a read volatile operation to make
        // a happen-before, because the init() method finally wrote 'this.started'
        if (!this.started) {
            throw new RheaRuntimeException("rhea kv is not started or shutdown");
        }
    }

    /**
     * storeEngine 的级别比 regionEngine 大 同时每个region对应一个 regionEngine
     * @param regionId
     * @return
     */
    private RegionEngine getRegionEngine(final long regionId) {
        if (this.storeEngine == null) {
            return null;
        }
        // 通过地区id 查询到 引擎对象
        return this.storeEngine.getRegionEngine(regionId);
    }

    /**
     * 通过区域id 查询区域引擎
     * @param regionId
     * @param requireLeader
     * @return
     */
    private RegionEngine getRegionEngine(final long regionId, final boolean requireLeader) {
        final RegionEngine engine = getRegionEngine(regionId);
        if (engine == null) {
            return null;
        }
        // 如果设置了 requireLeader 那么 引擎必须 isLeader = true
        if (requireLeader && !engine.isLeader()) {
            return null;
        }
        return engine;
    }

    /**
     * 尝试根据regionEngine 来获取leader
     * @param regionId
     * @return
     */
    private Endpoint getLeaderByRegionEngine(final long regionId) {
        // 从storeEngine 的 table 中找到 regionId 对应的 regionEngine对象
        final RegionEngine regionEngine = getRegionEngine(regionId);
        if (regionEngine != null) {
            // 访问 regionEngine关联的node 的leaderId
            final PeerId leader = regionEngine.getLeaderId();
            if (leader != null) {
                final String raftGroupId = JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), regionId);
                // 更新GroupConf 的 leader
                RouteTable.getInstance().updateLeader(raftGroupId, leader);
                return leader.getEndpoint();
            }
        }
        return null;
    }

    private RawKVStore getRawKVStore(final RegionEngine engine) {
        return engine.getMetricsRawKVStore();
    }

    private static boolean ensureOnValidEpoch(final Region region, final RegionEngine engine,
                                              final KVStoreClosure closure) {
        if (isValidEpoch(region, engine)) {
            return true;
        }
        // will retry on this error and status
        closure.setError(Errors.INVALID_REGION_EPOCH);
        closure.run(new Status(-1, "Invalid region epoch: %s", region));
        return false;
    }

    private static boolean isValidEpoch(final Region region, final RegionEngine engine) {
        return region.getRegionEpoch().equals(engine.getRegion().getRegionEpoch());
    }

    /**
     * 批量获取数据
     */
    private class GetBatching extends Batching<KeyEvent, byte[], byte[]> {

        public GetBatching(EventFactory<KeyEvent> factory, String name, EventHandler<KeyEvent> handler) {
            super(factory, batchingOpts.getBufSize(), name, handler);
        }

        @Override
        public boolean apply(final byte[] message, final CompletableFuture<byte[]> future) {
            return this.ringBuffer.tryPublishEvent((event, sequence) -> {
                event.reset();
                event.key = message;
                event.future = future;
            });
        }
    }

    private class PutBatching extends Batching<KVEvent, KVEntry, Boolean> {

        public PutBatching(EventFactory<KVEvent> factory, String name, PutBatchingHandler handler) {
            super(factory, batchingOpts.getBufSize(), name, handler);
        }

        @Override
        public boolean apply(final KVEntry message, final CompletableFuture<Boolean> future) {
            return this.ringBuffer.tryPublishEvent((event, sequence) -> {
                event.reset();
                event.kvEntry = message;
                event.future = future;
            });
        }
    }

    /**
     * 批量拉取数据 处理器
     */
    private class GetBatchingHandler extends AbstractBatchingHandler<KeyEvent> {

        /**
         * 判断是否要在获取了 readIndex 后才允许读取
         */
        private final boolean readOnlySafe;

        private GetBatchingHandler(String metricsName, boolean readOnlySafe) {
            super(metricsName);
            this.readOnlySafe = readOnlySafe;
        }

        /**
         * 处理批量拉取事件
         * @param event
         * @param sequence
         * @param endOfBatch  推测 如果该值为false 就先存储事件 当执行到默认时批量执行 这个套路可以理解为Disruptor 的最佳实践吧
         * @throws Exception
         */
        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(final KeyEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            // 在接受到事件后存储到对列中
            this.events.add(event);
            // 增加当前缓存的长度
            this.cachedBytes += event.key.length;
            final int size = this.events.size();
            // 没有达到最大缓存上限 且endOfBatch 为false 时 代表此时生产者 囤积了很多任务 这里可以批量执行
            if (!endOfBatch && size < batchingOpts.getBatchSize() && this.cachedBytes < batchingOpts.getMaxReadBytes()) {
                return;
            }

            if (size == 1) {
                // 统计相关的先不看
                reset();
                try {
                    // 执行单个任务并将结果设置到 future 中
                    get(event.key, this.readOnlySafe, event.future, false);
                } catch (final Throwable t) {
                    exceptionally(t, event.future);
                }
            } else {
                final List<byte[]> keys = Lists.newArrayListWithCapacity(size);
                final CompletableFuture<byte[]>[] futures = new CompletableFuture[size];
                for (int i = 0; i < size; i++) {
                    final KeyEvent e = this.events.get(i);
                    keys.add(e.key);
                    futures[i] = e.future;
                }
                reset();
                try {
                    // 批量获取 同时在完成任务时 将结果设置到future 中
                    multiGet(keys, this.readOnlySafe).whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            for (int i = 0; i < futures.length; i++) {
                                final ByteArray realKey = ByteArray.wrap(keys.get(i));
                                futures[i].complete(result.get(realKey));
                            }
                            return;
                        }
                        exceptionally(throwable, futures);
                    });
                } catch (final Throwable t) {
                    exceptionally(t, futures);
                }
            }
        }
    }

    /**
     * 批量拉取数据的任务
     */
    private class PutBatchingHandler extends AbstractBatchingHandler<KVEvent> {

        public PutBatchingHandler(String metricsName) {
            super(metricsName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(final KVEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            this.events.add(event);
            this.cachedBytes += event.kvEntry.length();
            final int size = this.events.size();
            if (!endOfBatch && size < batchingOpts.getBatchSize() && this.cachedBytes < batchingOpts.getMaxWriteBytes()) {
                return;
            }

            if (size == 1) {
                reset();
                final KVEntry kv = event.kvEntry;
                try {
                    put(kv.getKey(), kv.getValue(), event.future, false);
                } catch (final Throwable t) {
                    exceptionally(t, event.future);
                }
            } else {
                final List<KVEntry> entries = Lists.newArrayListWithCapacity(size);
                final CompletableFuture<Boolean>[] futures = new CompletableFuture[size];
                for (int i = 0; i < size; i++) {
                    final KVEvent e = this.events.get(i);
                    entries.add(e.kvEntry);
                    futures[i] = e.future;
                }
                reset();
                try {
                    put(entries).whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            for (int i = 0; i < futures.length; i++) {
                                futures[i].complete(result);
                            }
                            return;
                        }
                        exceptionally(throwable, futures);
                    });
                } catch (final Throwable t) {
                    exceptionally(t, futures);
                }
            }
        }
    }

    /**
     * 批量任务 处理器骨架类  包含批量拉取 和 设置
     * @param <T>
     */
    private abstract class AbstractBatchingHandler<T> implements EventHandler<T> {

        protected final Histogram histogramWithKeys;
        protected final Histogram histogramWithBytes;

        /**
         * 初始化 与 最多执行批量任务数等大的列表
         */
        protected final List<T>   events      = Lists.newArrayListWithCapacity(batchingOpts.getBatchSize());
        protected int             cachedBytes = 0;

        public AbstractBatchingHandler(String metricsName) {
            this.histogramWithKeys = KVMetrics.histogram(KVMetricNames.SEND_BATCHING, metricsName + "_keys");
            this.histogramWithBytes = KVMetrics.histogram(KVMetricNames.SEND_BATCHING, metricsName + "_bytes");
        }

        /**
         * 使用指定的异常触发所有 future
         * @param t
         * @param futures
         */
        public void exceptionally(final Throwable t, final CompletableFuture<?>... futures) {
            for (int i = 0; i < futures.length; i++) {
                futures[i].completeExceptionally(t);
            }
        }

        public void reset() {
            this.histogramWithKeys.update(this.events.size());
            this.histogramWithBytes.update(this.cachedBytes);

            this.events.clear();
            this.cachedBytes = 0;
        }
    }

    /**
     * 用key 来标识一个 future 对象
     */
    private static class KeyEvent {

        private byte[]                    key;
        private CompletableFuture<byte[]> future;

        public void reset() {
            this.key = null;
            this.future = null;
        }
    }

    private static class KVEvent {

        private KVEntry                    kvEntry;
        private CompletableFuture<Boolean> future;

        public void reset() {
            this.kvEntry = null;
            this.future = null;
        }
    }

    /**
     * @param <T>
     * @param <E>
     * @param <F>
     */
    private static abstract class Batching<T, E, F> {

        /**
         * 标识该批量任务
         */
        protected final String        name;
        /**
         * 并发队列
         */
        protected final Disruptor<T>  disruptor;
        /**
         * 环形缓冲区
         */
        protected final RingBuffer<T> ringBuffer;

        @SuppressWarnings("unchecked")
        public Batching(EventFactory<T> factory, int bufSize, String name, EventHandler<T> handler) {
            this.name = name;
            this.disruptor = new Disruptor<>(factory, bufSize, new NamedThreadFactory(name, true));
            this.disruptor.handleEventsWith(handler);
            // 该异常处理器 在发现异常时打印日志
            this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(name));
            this.ringBuffer = this.disruptor.start();
        }

        /**
         * 将message 以及对应的 future 发布到ringBuffer 中
         * @param message
         * @param future
         * @return
         */
        public abstract boolean apply(final E message, final CompletableFuture<F> future);

        public void shutdown() {
            try {
                this.disruptor.shutdown(3L, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOG.error("Fail to shutdown {}, {}.", toString(), StackTraceUtil.stackTrace(e));
            }
        }

        @Override
        public String toString() {
            return "Batching{" + "name='" + name + '\'' + ", disruptor=" + disruptor + '}';
        }
    }
}
