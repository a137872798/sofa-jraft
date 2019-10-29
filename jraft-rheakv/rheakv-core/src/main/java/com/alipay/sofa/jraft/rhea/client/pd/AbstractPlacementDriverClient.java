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
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.client.RegionRouteTable;
import com.alipay.sofa.jraft.rhea.client.RoundRobinLoadBalancer;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.configured.RpcOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.impl.AbstractBoltClientService;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;

/**
 * 安置驱动 骨架类
 * @author jiachun.fjc
 */
public abstract class AbstractPlacementDriverClient implements PlacementDriverClient {

    private static final Logger         LOG              = LoggerFactory.getLogger(AbstractPlacementDriverClient.class);

    /**
     * region 路由表  内部维护 startKey[] 与regionId    regionId 与 region 的映射关系
     */
    protected final RegionRouteTable    regionRouteTable = new RegionRouteTable();
    protected final long                clusterId;
    protected final String              clusterName;

    /**
     * 命令行服务
     */
    protected CliService                cliService;
    /**
     * 命令行客户端
     */
    protected CliClientService          cliClientService;
    /**
     *
     */
    protected RpcClient                 rpcClient;
    /**
     * 驱动服务对象 应该是将具体的行为抽象出来
     * 包含一个访问 PDclient 的api
     */
    protected PlacementDriverRpcService pdRpcService;

    protected AbstractPlacementDriverClient(long clusterId, String clusterName) {
        this.clusterId = clusterId;
        this.clusterName = clusterName;
    }

    /**
     * 初始化
     * @param opts
     * @return
     */
    @Override
    public synchronized boolean init(final PlacementDriverOptions opts) {
        // 初始化 cli 客户端
        initCli(opts.getCliOptions());
        this.pdRpcService = new DefaultPlacementDriverRpcService(this);
        RpcOptions rpcOpts = opts.getPdRpcOptions();
        if (rpcOpts == null) {
            rpcOpts = RpcOptionsConfigured.newDefaultConfig();
            rpcOpts.setCallbackExecutorCorePoolSize(0);
            rpcOpts.setCallbackExecutorMaximumPoolSize(0);
        }
        // 就是创建一个线程池
        if (!this.pdRpcService.init(rpcOpts)) {
            LOG.error("Fail to init [PlacementDriverRpcService].");
            return false;
        }
        // region route table
        // 解析路由表中的数据并填充
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = opts.getRegionRouteTableOptionsList();
        if (regionRouteTableOptionsList != null) {
            final String initialServerList = opts.getInitialServerList();
            for (final RegionRouteTableOptions regionRouteTableOpts : regionRouteTableOptionsList) {
                if (Strings.isBlank(regionRouteTableOpts.getInitialServerList())) {
                    // if blank, extends parent's value
                    regionRouteTableOpts.setInitialServerList(initialServerList);
                }
                // 初始化路由表
                initRouteTableByRegion(regionRouteTableOpts);
            }
        }
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.cliService != null) {
            this.cliService.shutdown();
        }
        if (this.pdRpcService != null) {
            this.pdRpcService.shutdown();
        }
    }

    @Override
    public long getClusterId() {
        return clusterId;
    }

    /**
     * 该对象相当于 对外界开放 用于获取region 的信息 而内部信息映射又是通过 regionRouteTable 和 regionTable 实现的
     * @param regionId
     * @return
     */
    @Override
    public Region getRegionById(final long regionId) {
        return this.regionRouteTable.getRegionById(regionId);
    }

    // 下面几个方法实际都是委托给 regionRouteTable 实现的

    /**
     * 通过某个key 查询region
     * @param key
     * @param forceRefresh  代表是否强制刷新
     * @return
     */
    @Override
    public Region findRegionByKey(final byte[] key, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionByKey(key);
    }

    @Override
    public Map<Region, List<byte[]>> findRegionsByKeys(final List<byte[]> keys, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionsByKeys(keys);
    }

    @Override
    public Map<Region, List<KVEntry>> findRegionsByKvEntries(final List<KVEntry> kvEntries, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionsByKvEntries(kvEntries);
    }

    @Override
    public List<Region> findRegionsByKeyRange(final byte[] startKey, final byte[] endKey, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionsByKeyRange(startKey, endKey);
    }

    @Override
    public byte[] findStartKeyOfNextRegion(final byte[] key, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findStartKeyOfNextRegion(key);
    }

    @Override
    public RegionRouteTable getRegionRouteTable() {
        return regionRouteTable;
    }

    /**
     * 更换leader
     * @param regionId
     * @param peer
     * @param refreshConf
     * @return
     */
    @Override
    public boolean transferLeader(final long regionId, final Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        // 将region 内相关的peer 信息取出来之后通过cliService 更换leader
        final Status status = this.cliService.transferLeader(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            // 如果需要在更改完成后刷新配置
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [transferLeader], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    /**
     * 添加一个 副本对象
     * @param regionId
     * @param peer
     * @param refreshConf
     * @return
     */
    @Override
    public boolean addReplica(final long regionId, final Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        // 添加 一个副本对象 就是在 group 中增加一个 peer 节点
        final Status status = this.cliService.addPeer(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [addReplica], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public boolean removeReplica(final long regionId, final Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        final Status status = this.cliService.removePeer(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [removeReplica], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        // 刷新leader
        PeerId leader = getLeader(raftGroupId, forceRefresh, timeoutMillis);
        // 如果leader信息还不存在 且没有设置强制刷新 (因为设置了强制刷新 上面就已经重新加载过了 还是没有只能说明本身leader 还没选举完成)
        if (leader == null && !forceRefresh) {
            // Could not found leader from cache, try again and force refresh cache
            leader = getLeader(raftGroupId, true, timeoutMillis);
        }
        if (leader == null) {
            throw new RouteTableException("no leader in group: " + raftGroupId);
        }
        return leader.getEndpoint();
    }

    /**
     * 获取某个组中的leader
     * @param raftGroupId
     * @param forceRefresh
     * @param timeoutMillis
     * @return
     */
    protected PeerId getLeader(final String raftGroupId, final boolean forceRefresh, final long timeoutMillis) {
        final RouteTable routeTable = RouteTable.getInstance();
        // 如果需要强制刷新
        if (forceRefresh) {
            final long deadline = System.currentTimeMillis() + timeoutMillis;
            final StringBuilder error = new StringBuilder();
            // A newly launched raft group may not have been successful in the election,
            // or in the 'leader-transfer' state, it needs to be re-tried
            Throwable lastCause = null;
            for (;;) {
                try {
                    // 就是通过groupId 找到对应的conf 并为conf 中每个节点发送获取leader 的请求 一旦成功返回结果
                    final Status st = routeTable.refreshLeader(this.cliClientService, raftGroupId, 2000);
                    if (st.isOk()) {
                        break;
                    }
                    error.append(st.toString());
                } catch (final InterruptedException e) {
                    ThrowUtil.throwException(e);
                } catch (final Throwable t) {
                    lastCause = t;
                    error.append(t.getMessage());
                }
                if (System.currentTimeMillis() < deadline) {
                    LOG.debug("Fail to find leader, retry again, {}.", error);
                    error.append(", ");
                    try {
                        Thread.sleep(10);
                    } catch (final InterruptedException e) {
                        ThrowUtil.throwException(e);
                    }
                } else {
                    throw lastCause != null ? new RouteTableException(error.toString(), lastCause)
                        : new RouteTableException(error.toString());
                }
            }
        }
        // 这里会取出 上面 refreshLeader 设置的leader
        return routeTable.selectLeader(raftGroupId);
    }

    /**
     * 推测是从peers 中通过均衡负载算法 选出一个合适的节点作为新leader
     * @param regionId
     * @param forceRefresh
     * @param timeoutMillis
     * @param unExpect  代表不需要出现的leader 应该就是本次失效的节点吧
     * @return
     */
    @Override
    public Endpoint getLuckyPeer(final long regionId, final boolean forceRefresh, final long timeoutMillis,
                                 final Endpoint unExpect) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final RouteTable routeTable = RouteTable.getInstance();
        if (forceRefresh) {
            final long deadline = System.currentTimeMillis() + timeoutMillis;
            final StringBuilder error = new StringBuilder();
            // A newly launched raft group may not have been successful in the election,
            // or in the 'leader-transfer' state, it needs to be re-tried
            for (;;) {
                try {
                    final Status st = routeTable.refreshConfiguration(this.cliClientService, raftGroupId, 5000);
                    if (st.isOk()) {
                        break;
                    }
                    error.append(st.toString());
                } catch (final InterruptedException e) {
                    ThrowUtil.throwException(e);
                } catch (final TimeoutException e) {
                    error.append(e.getMessage());
                }
                if (System.currentTimeMillis() < deadline) {
                    LOG.debug("Fail to get peers, retry again, {}.", error);
                    error.append(", ");
                    try {
                        Thread.sleep(5);
                    } catch (final InterruptedException e) {
                        ThrowUtil.throwException(e);
                    }
                } else {
                    throw new RouteTableException(error.toString());
                }
            }
        }
        final Configuration configs = routeTable.getConfiguration(raftGroupId);
        if (configs == null) {
            throw new RouteTableException("empty configs in group: " + raftGroupId);
        }
        final List<PeerId> peerList = configs.getPeers();
        if (peerList == null || peerList.isEmpty()) {
            throw new RouteTableException("empty peers in group: " + raftGroupId);
        }
        final int size = peerList.size();
        if (size == 1) {
            return peerList.get(0).getEndpoint();
        }
        // 获取均衡负载对象 从某个group 下所有的peer 中选择一个合适的
        final RoundRobinLoadBalancer balancer = RoundRobinLoadBalancer.getInstance(regionId);
        for (int i = 0; i < size; i++) {
            final PeerId candidate = balancer.select(peerList);
            final Endpoint luckyOne = candidate.getEndpoint();
            if (!luckyOne.equals(unExpect)) {
                return luckyOne;
            }
        }
        throw new RouteTableException("have no choice in group(peers): " + raftGroupId);
    }

    /**
     * 刷新路由配置
     * @param regionId
     */
    @Override
    public void refreshRouteConfiguration(final long regionId) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        try {
            // 更新leader
            getLeader(raftGroupId, true, 5000);
            // 根据当前leader 获取conf 信息
            RouteTable.getInstance().refreshConfiguration(this.cliClientService, raftGroupId, 5000);
        } catch (final Exception e) {
            LOG.error("Fail to refresh route configuration for {}, {}.", regionId, StackTraceUtil.stackTrace(e));
        }
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public PlacementDriverRpcService getPdRpcService() {
        return pdRpcService;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    /**
     * 将opts 中的映射关系添加到 regionTable 中
     * @param opts
     */
    protected void initRouteTableByRegion(final RegionRouteTableOptions opts) {
        final long regionId = Requires.requireNonNull(opts.getRegionId(), "opts.regionId");
        final byte[] startKey = opts.getStartKeyBytes();
        final byte[] endKey = opts.getEndKeyBytes();
        final String initialServerList = opts.getInitialServerList();
        final Region region = new Region();
        final Configuration conf = new Configuration();
        // region
        region.setId(regionId);
        region.setStartKey(startKey);
        region.setEndKey(endKey);
        // 初始化本region的描述信息
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        // peers
        Requires.requireTrue(Strings.isNotBlank(initialServerList), "opts.initialServerList is blank");
        conf.parse(initialServerList);
        region.setPeers(JRaftHelper.toPeerList(conf.listPeers()));
        // update raft route table
        // 创建一个 groupId 与 conf 的映射到 routeTable中
        RouteTable.getInstance().updateConfiguration(JRaftHelper.getJRaftGroupId(clusterName, regionId), conf);
        this.regionRouteTable.addOrUpdateRegion(region);
    }

    /**
     * 从opts 中抽取信息生成 region 对象之后添加到路由表中
     * @param opts
     * @return
     */
    protected Region getLocalRegionMetadata(final RegionEngineOptions opts) {
        final long regionId = Requires.requireNonNull(opts.getRegionId(), "opts.regionId");
        Requires.requireTrue(regionId >= Region.MIN_ID_WITH_MANUAL_CONF, "opts.regionId must >= "
                                                                         + Region.MIN_ID_WITH_MANUAL_CONF);
        Requires.requireTrue(regionId < Region.MAX_ID_WITH_MANUAL_CONF, "opts.regionId must < "
                                                                        + Region.MAX_ID_WITH_MANUAL_CONF);
        final byte[] startKey = opts.getStartKeyBytes();
        final byte[] endKey = opts.getEndKeyBytes();
        final String initialServerList = opts.getInitialServerList();
        final Region region = new Region();
        final Configuration conf = new Configuration();
        // region
        region.setId(regionId);
        region.setStartKey(startKey);
        region.setEndKey(endKey);
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        // peers
        Requires.requireTrue(Strings.isNotBlank(initialServerList), "opts.initialServerList is blank");
        conf.parse(initialServerList);
        region.setPeers(JRaftHelper.toPeerList(conf.listPeers()));
        this.regionRouteTable.addOrUpdateRegion(region);
        return region;
    }

    /**
     * 初始化 cli 服务
     * @param cliOpts
     */
    protected void initCli(CliOptions cliOpts) {
        if (cliOpts == null) {
            cliOpts = new CliOptions();
            cliOpts.setTimeoutMs(5000);
            cliOpts.setMaxRetry(3);
        }
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOpts);
        this.cliClientService = ((CliServiceImpl) this.cliService).getCliClientService();
        Requires.requireNonNull(this.cliClientService, "cliClientService");
        this.rpcClient = ((AbstractBoltClientService) this.cliClientService).getRpcClient();
    }

    /**
     * 刷新路由表的逻辑由子类实现 换句话说 怎么生成路由表中的数据也是子类实现
     */
    protected abstract void refreshRouteTable();
}
