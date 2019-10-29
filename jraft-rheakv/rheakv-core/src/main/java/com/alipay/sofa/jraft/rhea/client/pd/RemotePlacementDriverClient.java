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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * 访问远端的 PD???
 * @author jiachun.fjc
 */
public class RemotePlacementDriverClient extends AbstractPlacementDriverClient {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePlacementDriverClient.class);

    /**
     * pd 所在的组
     */
    private String              pdGroupId;
    /**
     * 发送元数据相关请求的client
     */
    private MetadataRpcClient   metadataRpcClient;

    /**
     * 是否已经启动
     */
    private boolean             started;

    public RemotePlacementDriverClient(long clusterId, String clusterName) {
        super(clusterId, clusterName);
    }

    @Override
    public synchronized boolean init(final PlacementDriverOptions opts) {
        if (this.started) {
            LOG.info("[RemotePlacementDriverClient] already started.");
            return true;
        }
        // 父类初始化会 填充路由表
        super.init(opts);
        this.pdGroupId = opts.getPdGroupId();
        if (Strings.isBlank(this.pdGroupId)) {
            throw new IllegalArgumentException("opts.pdGroup id must not be blank");
        }
        // 该值就是 conf 中的节点信息
        final String initialPdServers = opts.getInitialPdServerList();
        if (Strings.isBlank(initialPdServers)) {
            throw new IllegalArgumentException("opts.initialPdServerList must not be blank");
        }
        // 更新路由表中的 conf
        RouteTable.getInstance().updateConfiguration(this.pdGroupId, initialPdServers);
        // 初始化元数据访问client 并设置重试次数为3
        this.metadataRpcClient = new MetadataRpcClient(super.pdRpcService, 3);
        refreshRouteTable();
        LOG.info("[RemotePlacementDriverClient] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        super.shutdown();
        LOG.info("[RemotePlacementDriverClient] shutdown successfully.");
    }

    /**
     * 刷新路由表
     */
    @Override
    protected void refreshRouteTable() {
        // 通过metaClient 获取集群元数据信息
        final Cluster cluster = this.metadataRpcClient.getClusterInfo(this.clusterId);
        if (cluster == null) {
            LOG.warn("Cluster info is empty: {}.", this.clusterId);
            return;
        }
        // 获取该集群下所有存储对象
        final List<Store> stores = cluster.getStores();
        if (stores == null || stores.isEmpty()) {
            LOG.error("Stores info is empty: {}.", this.clusterId);
            return;
        }
        for (final Store store : stores) {
            // 获取每个store 下维护的所有region
            final List<Region> regions = store.getRegions();
            if (regions == null || regions.isEmpty()) {
                LOG.error("Regions info is empty: {} - {}.", this.clusterId, store.getId());
                continue;
            }
            // 更新regionRouteTable 的 region
            // regionRouteTable 内部维护一个TreeMap 同时每个region 有一个 startKey 和 endKey
            for (final Region region : regions) {
                super.regionRouteTable.addOrUpdateRegion(region);
            }
        }
    }

    /**
     * 获取 store 相关的元数据信息
     * @param opts
     * @return
     */
    @Override
    public Store getStoreMetadata(final StoreEngineOptions opts) {
        // 这里返回的是 store 的地址吗 store 算是 jraft 中的一个节点还是其他概念???
        final Endpoint selfEndpoint = opts.getServerAddress();
        // remote conf is the preferred
        final Store remoteStore = this.metadataRpcClient.getStoreInfo(this.clusterId, selfEndpoint);
        // 查询出来的同时 更新路由表
        if (!remoteStore.isEmpty()) {
            final List<Region> regions = remoteStore.getRegions();
            for (final Region region : regions) {
                super.regionRouteTable.addOrUpdateRegion(region);
            }
            return remoteStore;
        }
        // local conf
        // 代表使用selfEndpoint 没有拉取到数据 就创建本地存储对象
        final Store localStore = new Store();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = Lists.newArrayListWithCapacity(rOptsList.size());
        localStore.setId(remoteStore.getId());
        localStore.setEndpoint(selfEndpoint);
        for (final RegionEngineOptions rOpts : rOptsList) {
            // 通过opts 生成 Region 并保存到
            regionList.add(getLocalRegionMetadata(rOpts));
        }
        localStore.setRegions(regionList);
        // 更新 store 信息
        this.metadataRpcClient.updateStoreInfo(this.clusterId, localStore);
        return localStore;
    }

    @Override
    public Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis) {
        PeerId leader = getLeader(this.pdGroupId, forceRefresh, timeoutMillis);
        if (leader == null && !forceRefresh) {
            leader = getLeader(this.pdGroupId, true, timeoutMillis);
        }
        if (leader == null) {
            throw new RouteTableException("no placement driver leader in group: " + this.pdGroupId);
        }
        return new Endpoint(leader.getIp(), leader.getPort());
    }

    public MetadataRpcClient getMetadataRpcClient() {
        return metadataRpcClient;
    }
}
