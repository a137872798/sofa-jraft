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

import java.util.concurrent.CompletableFuture;

import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.client.failover.impl.FailoverClosureImpl;
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * 元数据客户端
 * @author jiachun.fjc
 */
public class MetadataRpcClient {

    /**
     * PD server 该对象只是定义了一个 将请求发送到某个地方的方法
     */
    private final PlacementDriverRpcService pdRpcService;
    /**
     * 故障转移次数
     */
    private final int                       failoverRetries;

    public MetadataRpcClient(PlacementDriverRpcService pdRpcService, int failoverRetries) {
        this.pdRpcService = pdRpcService;
        this.failoverRetries = failoverRetries;
    }

    /**
     * Returns the specified cluster information.
     * 通过集群id 生成requestId 并通过一个携带重试机制的 回调对象 尝试获取结果
     */
    public Cluster getClusterInfo(final long clusterId) {
        final CompletableFuture<Cluster> future = new CompletableFuture<>();
        internalGetClusterInfo(clusterId, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    /**
     * 内部获取集群信息
     * @param clusterId  当前集群id
     * @param future  用于设置结果的 future
     * @param retriesLeft  重试次数
     * @param lastCause  处理哪个异常
     */
    private void internalGetClusterInfo(final long clusterId, final CompletableFuture<Cluster> future,
                                        final int retriesLeft, final Errors lastCause) {
        // 定义了 针对某个Error 的重试逻辑
        final RetryRunner retryRunner = retryCause -> internalGetClusterInfo(clusterId, future,
                retriesLeft - 1, retryCause);
        // 创建一个故障转移回调
        final FailoverClosure<Cluster> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetClusterInfoRequest request = new GetClusterInfoRequest();
        request.setClusterId(clusterId);
        // 发送获取集群信息的请求 失败时 实际上通过回调对象在递归调用 internalGetClusterInfo
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * The pd server stores the storeIds of all nodes.
     * This method provides a lookup for the storeId according
     * to the host.  If there is no value, then a globally
     * unique storeId is created.
     * 同样通过一个 重试回调来调用
     */
    public Long getOrCreateStoreId(final long clusterId, final Endpoint endpoint) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalGetOrCreateStoreId(clusterId, endpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetOrCreateStoreId(final long clusterId, final Endpoint endpoint,
                                            final CompletableFuture<Long> future, final int retriesLeft,
                                            final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetOrCreateStoreId(clusterId, endpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        // 发送获取 storeId 的请求  store与 node 是什么关系???
        final GetStoreIdRequest request = new GetStoreIdRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(endpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    // 下面的方法套路类似 只是对应服务端的处理逻辑不同

    /**
     * Query the store information by the host.  If the result
     * is a empty instance, the caller needs to use its own local
     * configuration.
     */
    public Store getStoreInfo(final long clusterId, final Endpoint selfEndpoint) {
        final CompletableFuture<Store> future = new CompletableFuture<>();
        internalGetStoreInfo(clusterId, selfEndpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetStoreInfo(final long clusterId, final Endpoint selfEndpoint,
                                      final CompletableFuture<Store> future, final int retriesLeft,
                                      final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetStoreInfo(clusterId, selfEndpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Store> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetStoreInfoRequest request = new GetStoreInfoRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(selfEndpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Update the store information by the storeId.
     */
    public Store updateStoreInfo(final long clusterId, final Store store) {
        final CompletableFuture<Store> future = new CompletableFuture<>();
        internalUpdateStoreInfo(clusterId, store, future, 1, null);
        return FutureHelper.get(future);
    }

    private void internalUpdateStoreInfo(final long clusterId, final Store store, final CompletableFuture<Store> future,
                                         final int retriesLeft, final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalUpdateStoreInfo(clusterId, store, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Store> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final SetStoreInfoRequest request = new SetStoreInfoRequest();
        request.setClusterId(clusterId);
        request.setStore(store);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Create a globally unique regionId.
     */
    public Long createRegionId(final long clusterId, final Endpoint endpoint) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalCreateRegionId(clusterId, endpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalCreateRegionId(final long clusterId, final Endpoint endpoint,
                                        final CompletableFuture<Long> future, final int retriesLeft,
                                        final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalCreateRegionId(clusterId, endpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final CreateRegionIdRequest request = new CreateRegionIdRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(endpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }
}
