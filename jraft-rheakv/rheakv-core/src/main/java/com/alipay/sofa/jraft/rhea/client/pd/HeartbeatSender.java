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
package com.alipay.sofa.jraft.rhea.client.pd;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.errors.ErrorsHelper;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.StoreStats;
import com.alipay.sofa.jraft.rhea.metadata.TimeInterval;
import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DiscardOldPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.timer.HashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.TimerTask;

/**
 * 心跳发送对象
 *
 * @author jiachun.fjc
 */
public class HeartbeatSender implements Lifecycle<HeartbeatOptions> {

    private static final Logger         LOG = LoggerFactory.getLogger(HeartbeatSender.class);

    /**
     * 关联一个存储引擎
     */
    private final StoreEngine           storeEngine;
    /**
     * PD 客户端
     */
    private final PlacementDriverClient pdClient;
    /**
     * rpc 客户端
     */
    private final RpcClient             rpcClient;

    private StatsCollector              statsCollector;
    /**
     * 命令处理器
     */
    private InstructionProcessor        instructionProcessor;
    private int                         heartbeatRpcTimeoutMillis;
    private ThreadPoolExecutor          heartbeatRpcCallbackExecutor;
    /**
     * 定时器
     */
    private HashedWheelTimer            heartbeatTimer;

    private boolean                     started;

    public HeartbeatSender(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        this.pdClient = storeEngine.getPlacementDriverClient();
        this.rpcClient = ((AbstractPlacementDriverClient) this.pdClient).getRpcClient();
    }

    /**
     * 初始化 心跳检测发送
     * @param opts
     * @return
     */
    @Override
    public synchronized boolean init(final HeartbeatOptions opts) {
        if (this.started) {
            LOG.info("[HeartbeatSender] already started.");
            return true;
        }
        this.statsCollector = new StatsCollector(this.storeEngine);
        // 通过传入的 engine 初始化命令处理器 该对象内部包含拆分和 更替leader 的方法 实际都是委托给 engine 实现  其中数据更替的最小单位是 RegionEngine
        this.instructionProcessor = new InstructionProcessor(this.storeEngine);
        this.heartbeatTimer = new HashedWheelTimer(new NamedThreadFactory("heartbeat-timer", true), 50,
            TimeUnit.MILLISECONDS, 4096);
        this.heartbeatRpcTimeoutMillis = opts.getHeartbeatRpcTimeoutMillis();
        if (this.heartbeatRpcTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Heartbeat rpc timeout millis must > 0, "
                                               + this.heartbeatRpcTimeoutMillis);
        }
        final String name = "rheakv-heartbeat-callback";
        // 构建处理心跳回调的线程池
        this.heartbeatRpcCallbackExecutor = ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(true) //
            .coreThreads(4) //
            .maximumThreads(4) //
            .keepAliveSeconds(120L) //
            .workQueue(new ArrayBlockingQueue<>(1024)) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(new DiscardOldPolicyWithReport(name)) //
            .build();
        final long storeHeartbeatIntervalSeconds = opts.getStoreHeartbeatIntervalSeconds();
        final long regionHeartbeatIntervalSeconds = opts.getRegionHeartbeatIntervalSeconds();
        if (storeHeartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException("Store heartbeat interval seconds must > 0, "
                                               + storeHeartbeatIntervalSeconds);
        }
        if (regionHeartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException("Region heartbeat interval seconds must > 0, "
                                               + regionHeartbeatIntervalSeconds);
        }
        final long now = System.currentTimeMillis();
        // 初始化2个定时任务 并通过 hashWheel 执行
        final StoreHeartbeatTask storeHeartbeatTask = new StoreHeartbeatTask(storeHeartbeatIntervalSeconds, now, false);
        final RegionHeartbeatTask regionHeartbeatTask = new RegionHeartbeatTask(regionHeartbeatIntervalSeconds, now,
            false);
        this.heartbeatTimer.newTimeout(storeHeartbeatTask, storeHeartbeatTask.getNextDelay(), TimeUnit.SECONDS);
        this.heartbeatTimer.newTimeout(regionHeartbeatTask, regionHeartbeatTask.getNextDelay(), TimeUnit.SECONDS);
        LOG.info("[HeartbeatSender] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.heartbeatRpcCallbackExecutor);
        if (this.heartbeatTimer != null) {
            this.heartbeatTimer.stop();
        }
    }

    /**
     * 发送 store 的心跳
     * @param nextDelay
     * @param forceRefreshLeader
     * @param lastTime
     */
    private void sendStoreHeartbeat(final long nextDelay, final boolean forceRefreshLeader, final long lastTime) {
        final long now = System.currentTimeMillis();
        // 构建请求对象
        final StoreHeartbeatRequest request = new StoreHeartbeatRequest();
        // 为请求对象设置集群Id
        request.setClusterId(this.storeEngine.getClusterId());
        final TimeInterval timeInterval = new TimeInterval(lastTime, now);
        final StoreStats stats = this.statsCollector.collectStoreStats(timeInterval);
        request.setStats(stats);
        final HeartbeatClosure<Object> closure = new HeartbeatClosure<Object>() {

            @Override
            public void run(final Status status) {
                // 如果该异常代表是 无效节点的相关信息 那么 可以通过更新leader 后重新执行来避免再次出现相同问题
                // 在回调中开启下次任务
                final boolean forceRefresh = !status.isOk() && ErrorsHelper.isInvalidPeer(getError());
                final StoreHeartbeatTask nexTask = new StoreHeartbeatTask(nextDelay, now, forceRefresh);
                heartbeatTimer.newTimeout(nexTask, nexTask.getNextDelay(), TimeUnit.SECONDS);
            }
        };
        // 获取leader 的地址
        final Endpoint endpoint = this.pdClient.getPdLeader(forceRefreshLeader, this.heartbeatRpcTimeoutMillis);
        // 异步发送心跳 通过回调对象来处理逻辑
        callAsyncWithRpc(endpoint, request, closure);
    }

    /**
     * 针对 region 的心跳检测请求
     * @param nextDelay
     * @param lastTime
     * @param forceRefreshLeader
     */
    private void sendRegionHeartbeat(final long nextDelay, final long lastTime, final boolean forceRefreshLeader) {
        final long now = System.currentTimeMillis();
        final RegionHeartbeatRequest request = new RegionHeartbeatRequest();
        request.setClusterId(this.storeEngine.getClusterId());
        request.setStoreId(this.storeEngine.getStoreId());
        // 设置续约key  这里的续约是指什么???
        request.setLeastKeysOnSplit(this.storeEngine.getStoreOpts().getLeastKeysOnSplit());
        // 获取该storeEngine 管理的所有regionId
        final List<Long> regionIdList = this.storeEngine.getLeaderRegionIds();
        // 数据为空的时候设置下次任务 并退出本次执行 那么不会超过心跳限制时间吗
        if (regionIdList.isEmpty()) {
            // So sad, there is no even a region leader :(
            final RegionHeartbeatTask nextTask = new RegionHeartbeatTask(nextDelay, now, false);
            this.heartbeatTimer.newTimeout(nextTask, nextTask.getNextDelay(), TimeUnit.SECONDS);
            if (LOG.isInfoEnabled()) {
                LOG.info("So sad, there is no even a region leader on [clusterId:{}, storeId: {}, endpoint:{}].",
                    this.storeEngine.getClusterId(), this.storeEngine.getStoreId(), this.storeEngine.getSelfEndpoint());
            }
            return;
        }
        final List<Pair<Region, RegionStats>> regionStatsList = Lists.newArrayListWithCapacity(regionIdList.size());
        // 代表上次执行任务距离本次的时间间隔
        final TimeInterval timeInterval = new TimeInterval(lastTime, now);
        for (final Long regionId : regionIdList) {
            // 找到对应的region
            final Region region = this.pdClient.getRegionById(regionId);
            // 生成统计对象并维护关联关系
            final RegionStats stats = this.statsCollector.collectRegionStats(region, timeInterval);
            if (stats == null) {
                continue;
            }
            regionStatsList.add(Pair.of(region, stats));
        }
        request.setRegionStatsList(regionStatsList);
        final HeartbeatClosure<List<Instruction>> closure = new HeartbeatClosure<List<Instruction>>() {

            @Override
            public void run(final Status status) {
                final boolean isOk = status.isOk();
                if (isOk) {
                    // 获取一组命令
                    final List<Instruction> instructions = getResult();
                    if (instructions != null && !instructions.isEmpty()) {
                        // 处理任务
                        instructionProcessor.process(instructions);
                    }
                }
                // 如果发生的异常是 leader无效相关的 就设置下次任务
                final boolean forceRefresh = !isOk && ErrorsHelper.isInvalidPeer(getError());
                final RegionHeartbeatTask nextTask = new RegionHeartbeatTask(nextDelay, now, forceRefresh);
                heartbeatTimer.newTimeout(nextTask, nextTask.getNextDelay(), TimeUnit.SECONDS);
            }
        };
        final Endpoint endpoint = this.pdClient.getPdLeader(forceRefreshLeader, this.heartbeatRpcTimeoutMillis);
        callAsyncWithRpc(endpoint, request, closure);
    }

    /**
     * 以异步方式调用
     * @param endpoint
     * @param request
     * @param closure
     * @param <V>
     */
    private <V> void callAsyncWithRpc(final Endpoint endpoint, final BaseRequest request,
                                      final HeartbeatClosure<V> closure) {
        final String address = endpoint.toString();
        final InvokeContext invokeCtx = ExtSerializerSupports.getInvokeContext();
        // 该回调对象是 remoting 中 指定的回调对象
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @SuppressWarnings("unchecked")
            @Override
            public void onResponse(final Object result) {
                final BaseResponse<?> response = (BaseResponse<?>) result;
                if (response.isSuccess()) {
                    closure.setResult((V) response.getValue());
                    closure.run(Status.OK());
                } else {
                    closure.setError(response.getError());
                    closure.run(new Status(-1, "RPC failed with address: %s, response: %s", address, response));
                }
            }

            @Override
            public void onException(final Throwable t) {
                closure.run(new Status(-1, t.getMessage()));
            }

            @Override
            public Executor getExecutor() {
                return heartbeatRpcCallbackExecutor;
            }
        };
        try {
            this.rpcClient.invokeWithCallback(address, request, invokeCtx, invokeCallback,
                this.heartbeatRpcTimeoutMillis);
        } catch (final Throwable t) {
            closure.run(new Status(-1, t.getMessage()));
        }
    }

    private static abstract class HeartbeatClosure<V> extends BaseKVStoreClosure {

        private volatile V result;

        public V getResult() {
            return result;
        }

        public void setResult(V result) {
            this.result = result;
        }
    }

    /**
     * 发送store 的 心跳
     */
    private final class StoreHeartbeatTask implements TimerTask {

        /**
         * 代表每次发送的时间间隔
         */
        private final long    nextDelay;
        /**
         * 上次发送的结束时间
         */
        private final long    lastTime;
        /**
         * 是否强制刷新leader
         */
        private final boolean forceRefreshLeader;

        private StoreHeartbeatTask(long nextDelay, long lastTime, boolean forceRefreshLeader) {
            this.nextDelay = nextDelay;
            this.lastTime = lastTime;
            this.forceRefreshLeader = forceRefreshLeader;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendStoreHeartbeat(this.nextDelay, this.forceRefreshLeader, this.lastTime);
            } catch (final Throwable t) {
                LOG.error("Caught a error on sending [StoreHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

        public long getNextDelay() {
            return nextDelay;
        }
    }

    /**
     * 关于 region 的心跳检测任务
     */
    private final class RegionHeartbeatTask implements TimerTask {

        private final long    nextDelay;
        private final long    lastTime;
        private final boolean forceRefreshLeader;

        private RegionHeartbeatTask(long nextDelay, long lastTime, boolean forceRefreshLeader) {
            this.nextDelay = nextDelay;
            this.lastTime = lastTime;
            this.forceRefreshLeader = forceRefreshLeader;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendRegionHeartbeat(this.nextDelay, this.lastTime, this.forceRefreshLeader);
            } catch (final Throwable t) {
                LOG.error("Caught a error on sending [RegionHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

        public long getNextDelay() {
            return nextDelay;
        }
    }
}
