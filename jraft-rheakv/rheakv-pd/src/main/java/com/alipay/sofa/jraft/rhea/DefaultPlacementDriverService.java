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
package com.alipay.sofa.jraft.rhea;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatResponse;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.alipay.sofa.jraft.rhea.pipeline.event.RegionPingEvent;
import com.alipay.sofa.jraft.rhea.pipeline.event.StorePingEvent;
import com.alipay.sofa.jraft.rhea.pipeline.handler.LogHandler;
import com.alipay.sofa.jraft.rhea.pipeline.handler.PlacementDriverTailHandler;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.CallerRunsPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.rhea.util.pipeline.DefaultHandlerInvoker;
import com.alipay.sofa.jraft.rhea.util.pipeline.DefaultPipeline;
import com.alipay.sofa.jraft.rhea.util.pipeline.Handler;
import com.alipay.sofa.jraft.rhea.util.pipeline.HandlerInvoker;
import com.alipay.sofa.jraft.rhea.util.pipeline.Pipeline;
import com.alipay.sofa.jraft.rhea.util.pipeline.future.PipelineFuture;
import com.alipay.sofa.jraft.util.JRaftServiceLoader;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;

/**
 * 默认的 PD service  看来该对象也是专门抽取出来用于做其他事的
 * @author jiachun.fjc
 */
public class DefaultPlacementDriverService implements PlacementDriverService, LeaderStateListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPlacementDriverService.class);

    /**
     * kv存储对象 可以访问到 RocksDB
     */
    private final RheaKVStore   rheaKVStore;

    /**
     * 抽象出来专门获取 store cluster region 相关信息的
     */
    private MetadataStore       metadataStore;
    /**
     * 责任链上每一环节的处理器
     */
    private HandlerInvoker      pipelineInvoker;
    /**
     * 责任链模式处理任务
     */
    private Pipeline            pipeline;
    private volatile boolean    isLeader;

    private boolean             started;

    public DefaultPlacementDriverService(RheaKVStore rheaKVStore) {
        this.rheaKVStore = rheaKVStore;
    }

    /**
     * 初始化
     * @param opts
     * @return
     */
    @Override
    public synchronized boolean init(final PlacementDriverServerOptions opts) {
        if (this.started) {
            LOG.info("[DefaultPlacementDriverService] already started.");
            return true;
        }
        Requires.requireNonNull(opts, "placementDriverServerOptions");
        // 初始化元数据存储对象 他们共用一个 kvStore
        this.metadataStore = new DefaultMetadataStore(this.rheaKVStore);
        // 创建 责任链
        final ThreadPoolExecutor threadPool = createPipelineExecutor(opts);
        if (threadPool != null) {
            this.pipelineInvoker = new DefaultHandlerInvoker(threadPool);
        }
        this.pipeline = new DefaultPipeline(); //
        initPipeline(this.pipeline);

        LOG.info("[DefaultPlacementDriverService] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        try {
            if (this.pipelineInvoker != null) {
                this.pipelineInvoker.shutdown();
            }
            invalidLocalCache();
        } finally {
            this.started = false;
            LOG.info("[DefaultPlacementDriverService] shutdown successfully.");
        }
    }

    @Override
    public void handleStoreHeartbeatRequest(final StoreHeartbeatRequest request,
                                            final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final StoreHeartbeatResponse response = new StoreHeartbeatResponse();
        response.setClusterId(request.getClusterId());
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            // Only save the data
            final StorePingEvent storePingEvent = new StorePingEvent(request, this.metadataStore);
            final PipelineFuture<Object> future = this.pipeline.invoke(storePingEvent);
            future.whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(throwable));
                    response.setError(Errors.forException(throwable));
                }
                closure.sendResponse(response);
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleRegionHeartbeatRequest(final RegionHeartbeatRequest request,
                                             final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final RegionHeartbeatResponse response = new RegionHeartbeatResponse();
        response.setClusterId(request.getClusterId());
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            // 1. First, save the data
            // 2. Second, check if need to send a dispatch instruction
            final RegionPingEvent regionPingEvent = new RegionPingEvent(request, this.metadataStore);
            final PipelineFuture<List<Instruction>> future = this.pipeline.invoke(regionPingEvent);
            future.whenComplete((instructions, throwable) -> {
                if (throwable == null) {
                    response.setValue(instructions);
                } else {
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(throwable));
                    response.setError(Errors.forException(throwable));
                }
                closure.sendResponse(response);
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleGetClusterInfoRequest(final GetClusterInfoRequest request,
                                            final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final GetClusterInfoResponse response = new GetClusterInfoResponse();
        response.setClusterId(clusterId);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final Cluster cluster = this.metadataStore.getClusterInfo(clusterId);
            response.setCluster(cluster);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    @Override
    public void handleGetStoreInfoRequest(final GetStoreInfoRequest request,
                                          final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final GetStoreInfoResponse response = new GetStoreInfoResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final Store store = this.metadataStore.getStoreInfo(clusterId, request.getEndpoint());
            response.setValue(store);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    @Override
    public void handleSetStoreInfoRequest(final SetStoreInfoRequest request,
                                          final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final SetStoreInfoResponse response = new SetStoreInfoResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final CompletableFuture<Store> future = this.metadataStore.updateStoreInfo(clusterId, request.getStore());
            future.whenComplete((prevStore, throwable) -> {
                if (throwable == null) {
                    response.setValue(prevStore);
                } else {
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(throwable));
                    response.setError(Errors.forException(throwable));
                }
                closure.sendResponse(response);
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleGetStoreIdRequest(final GetStoreIdRequest request,
                                        final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final GetStoreIdResponse response = new GetStoreIdResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final Long storeId = this.metadataStore.getOrCreateStoreId(clusterId, request.getEndpoint());
            response.setValue(storeId);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    @Override
    public void handleCreateRegionIdRequest(final CreateRegionIdRequest request,
                                            final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final CreateRegionIdResponse response = new CreateRegionIdResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final Long newRegionId = this.metadataStore.createRegionId(clusterId);
            response.setValue(newRegionId);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    @Override
    public void onLeaderStart(final long leaderTerm) {
        this.isLeader = true;
        invalidLocalCache();
    }

    @Override
    public void onLeaderStop(final long leaderTerm) {
        this.isLeader = false;
        invalidLocalCache();
    }

    protected void initPipeline(final Pipeline pipeline) {
        final List<Handler> sortedHandlers = JRaftServiceLoader.load(Handler.class) //
            .sort();

        // default handlers and order:
        //
        // 1. logHandler
        // 2. storeStatsValidator
        // 3. regionStatsValidator
        // 4. storeStatsPersistence
        // 5. regionStatsPersistence
        // 6. regionLeaderBalance
        // 7. splittingJudgeByApproximateKeys
        // 8: placementDriverTail
        for (final Handler h : sortedHandlers) {
            pipeline.addLast(h);
        }

        // first handler
        pipeline.addFirst(this.pipelineInvoker, "logHandler", new LogHandler());
        // last handler
        pipeline.addLast("placementDriverTail", new PlacementDriverTailHandler());
    }

    private void invalidLocalCache() {
        if (this.metadataStore != null) {
            this.metadataStore.invalidCache();
        }
        ClusterStatsManager.invalidCache();
    }

    /**
     * 创建责任链专用的 线程池
     * @param opts
     * @return
     */
    private ThreadPoolExecutor createPipelineExecutor(final PlacementDriverServerOptions opts) {
        final int corePoolSize = opts.getPipelineCorePoolSize();
        final int maximumPoolSize = opts.getPipelineMaximumPoolSize();
        if (corePoolSize <= 0 || maximumPoolSize <= 0) {
            return null;
        }

        final String name = "rheakv-pipeline-executor";
        return ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(false) //
            .coreThreads(corePoolSize) //
            .maximumThreads(maximumPoolSize) //
            .keepAliveSeconds(120L) //
            .workQueue(new ArrayBlockingQueue<>(1024)) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(new CallerRunsPolicyWithReport(name)) //
            .build();
    }
}
