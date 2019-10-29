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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.client.pd.HeartbeatSender;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.RemotePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.rhea.options.MemoryDBOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.BatchRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVClosureAdapter;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.MemoryRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.NetUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.MetricThreadPoolExecutor;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;

/**
 * Storage engine, there is only one instance in a node,
 * containing one or more {@link RegionEngine}.
 * 存储引擎  一个节点中只有一个  内部包含一个或多个 RegionEngine
 * @author jiachun.fjc
 */
public class StoreEngine implements Lifecycle<StoreEngineOptions> {

    private static final Logger                        LOG                  = LoggerFactory
                                                                                .getLogger(StoreEngine.class);

    static {
        ExtSerializerSupports.init();
    }

    /**
     * key: regionId  value: 对应的 kv 服务器
     */
    private final ConcurrentMap<Long, RegionKVService> regionKVServiceTable = Maps.newConcurrentMapLong();
    /**
     * key: regionId value: 地区引擎
     */
    private final ConcurrentMap<Long, RegionEngine>    regionEngineTable    = Maps.newConcurrentMapLong();
    /**
     * PD客户端
     */
    private final PlacementDriverClient                pdClient;
    private final long                                 clusterId;

    private Long                                       storeId;
    private final AtomicBoolean                        splitting            = new AtomicBoolean(false);
    // When the store is started (unix timestamp in milliseconds)
    private long                                       startTime            = System.currentTimeMillis();
    /**
     * DB 文件
     */
    private File                                       dbPath;
    /**
     * RPC 服务器
     */
    private RpcServer                                  rpcServer;
    /**
     * 具备批量存储功能  也就是存储引擎看作是一个具备存储能力的服务器 核心能力由该对象实现 而其他组件是它作为服务器额外的功能
     */
    private BatchRawKVStore<?>                         rawKVStore;
    /**
     * 发送心跳对象
     */
    private HeartbeatSender                            heartbeatSender;
    private StoreEngineOptions                         storeOpts;

    // Shared executor services
    private ExecutorService                            readIndexExecutor;
    private ExecutorService                            raftStateTrigger;
    private ExecutorService                            snapshotExecutor;
    private ExecutorService                            cliRpcExecutor;
    private ExecutorService                            raftRpcExecutor;
    private ExecutorService                            kvRpcExecutor;

    private ScheduledExecutorService                   metricsScheduler;
    // 测量相关的
    private ScheduledReporter                          kvMetricsReporter;
    private ScheduledReporter                          threadPoolMetricsReporter;

    private boolean                                    started;

    public StoreEngine(PlacementDriverClient pdClient) {
        this.pdClient = pdClient;
        this.clusterId = pdClient.getClusterId();
    }

    /**
     * 初始化存储引擎
     * @param opts
     * @return
     */
    @Override
    public synchronized boolean init(final StoreEngineOptions opts) {
        if (this.started) {
            LOG.info("[StoreEngine] already started.");
            return true;
        }
        this.storeOpts = Requires.requireNonNull(opts, "opts");
        Endpoint serverAddress = Requires.requireNonNull(opts.getServerAddress(), "opts.serverAddress");
        final int port = serverAddress.getPort();
        final String ip = serverAddress.getIp();
        if (ip == null || Utils.IP_ANY.equals(ip)) {
            // 生成服务器地址
            serverAddress = new Endpoint(NetUtil.getLocalCanonicalHostName(), port);
            opts.setServerAddress(serverAddress);
        }
        final long metricsReportPeriod = opts.getMetricsReportPeriod();
        // init region options  初始化 以region  为单位的存储对象
        List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        if (rOptsList == null || rOptsList.isEmpty()) {
            // -1 region  代表没有regionEngine相关配置 就创建一个默认值
            final RegionEngineOptions rOpts = new RegionEngineOptions();
            rOpts.setRegionId(Constants.DEFAULT_REGION_ID);
            rOptsList = Lists.newArrayList();
            rOptsList.add(rOpts);
            opts.setRegionEngineOptionsList(rOptsList);
        }
        // 获取集群名
        final String clusterName = this.pdClient.getClusterName();
        // 生成regionEngine   看来regionEngine 是属于某个 storeEngine 的 一个storeEngine 下有多个regionEngine
        for (final RegionEngineOptions rOpts : rOptsList) {
            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(clusterName, rOpts.getRegionId()));
            rOpts.setServerAddress(serverAddress);
            // 初始化服务组
            rOpts.setInitialServerList(opts.getInitialServerList());
            if (rOpts.getNodeOptions() == null) {
                // copy common node options
                // 没有配置就使用 公共配置
                rOpts.setNodeOptions(opts.getCommonNodeOptions() == null ? new NodeOptions() : opts
                    .getCommonNodeOptions().copy());
            }
            if (rOpts.getMetricsReportPeriod() <= 0 && metricsReportPeriod > 0) {
                // extends store opts
                rOpts.setMetricsReportPeriod(metricsReportPeriod);
            }
        }
        // init store 通过 pdClient 内部的 metadataRpcClient 以及opts 的 serverEndpoint 去 拉取store信息 同时会更新regionRouteTable
        final Store store = this.pdClient.getStoreMetadata(opts);
        if (store == null || store.getRegions() == null || store.getRegions().isEmpty()) {
            LOG.error("Empty store metadata: {}.", store);
            return false;
        }
        this.storeId = store.getId();
        // init executors  创建相关线程池
        if (this.readIndexExecutor == null) {
            this.readIndexExecutor = StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
        }
        if (this.raftStateTrigger == null) {
            this.raftStateTrigger = StoreEngineHelper.createRaftStateTrigger(opts.getLeaderStateTriggerCoreThreads());
        }
        if (this.snapshotExecutor == null) {
            this.snapshotExecutor = StoreEngineHelper.createSnapshotExecutor(opts.getSnapshotCoreThreads());
        }
        // init rpc executors
        final boolean useSharedRpcExecutor = opts.isUseSharedRpcExecutor();
        if (!useSharedRpcExecutor) {
            if (this.cliRpcExecutor == null) {
                this.cliRpcExecutor = StoreEngineHelper.createCliRpcExecutor(opts.getCliRpcCoreThreads());
            }
            if (this.raftRpcExecutor == null) {
                this.raftRpcExecutor = StoreEngineHelper.createRaftRpcExecutor(opts.getRaftRpcCoreThreads());
            }
            if (this.kvRpcExecutor == null) {
                this.kvRpcExecutor = StoreEngineHelper.createKvRpcExecutor(opts.getKvRpcCoreThreads());
            }
        }
        // init metrics
        startMetricReporters(metricsReportPeriod);
        // init rpc server  创建RpcServer 用于接受远端的请求
        this.rpcServer = new RpcServer(port, true, true);
        // 注册请求处理器  raftRpcExecutor 代表处理 raft 请求 cli 代表处理命令行请求
        RaftRpcServerFactory.addRaftRequestProcessors(this.rpcServer, this.raftRpcExecutor, this.cliRpcExecutor);
        // 本对象具备处理多种req 的能力
        StoreEngineHelper.addKvStoreRequestProcessor(this.rpcServer, this);
        // 启动netty 服务器
        if (!this.rpcServer.start()) {
            LOG.error("Fail to init [RpcServer].");
            return false;
        }
        // init db store  初始化KVStore 失败
        if (!initRawKVStore(opts)) {
            return false;
        }
        // init all region engine
        // 初始化该storeEngine 下所有的 regionEngine
        if (!initAllRegionEngine(opts, store)) {
            LOG.error("Fail to init all [RegionEngine].");
            return false;
        }
        // heartbeat sender
        if (this.pdClient instanceof RemotePlacementDriverClient) {
            HeartbeatOptions heartbeatOpts = opts.getHeartbeatOptions();
            if (heartbeatOpts == null) {
                heartbeatOpts = new HeartbeatOptions();
            }
            this.heartbeatSender = new HeartbeatSender(this);
            if (!this.heartbeatSender.init(heartbeatOpts)) {
                LOG.error("Fail to init [HeartbeatSender].");
                return false;
            }
        }
        this.startTime = System.currentTimeMillis();
        LOG.info("[StoreEngine] start successfully: {}.", this);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.rpcServer != null) {
            this.rpcServer.stop();
        }
        if (!this.regionEngineTable.isEmpty()) {
            for (final RegionEngine engine : this.regionEngineTable.values()) {
                engine.shutdown();
            }
            this.regionEngineTable.clear();
        }
        if (this.rawKVStore != null) {
            this.rawKVStore.shutdown();
        }
        if (this.heartbeatSender != null) {
            this.heartbeatSender.shutdown();
        }
        this.regionKVServiceTable.clear();
        if (this.kvMetricsReporter != null) {
            this.kvMetricsReporter.stop();
        }
        if (this.threadPoolMetricsReporter != null) {
            this.threadPoolMetricsReporter.stop();
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.readIndexExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftStateTrigger);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.snapshotExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.cliRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.kvRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.metricsScheduler);
        this.started = false;
        LOG.info("[StoreEngine] shutdown successfully.");
    }

    public PlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    public long getClusterId() {
        return clusterId;
    }

    public Long getStoreId() {
        return storeId;
    }

    public StoreEngineOptions getStoreOpts() {
        return storeOpts;
    }

    public long getStartTime() {
        return startTime;
    }

    public RpcServer getRpcServer() {
        return rpcServer;
    }

    public BatchRawKVStore<?> getRawKVStore() {
        return rawKVStore;
    }

    public RegionKVService getRegionKVService(final long regionId) {
        return this.regionKVServiceTable.get(regionId);
    }

    public long getTotalSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getTotalSpace();
    }

    public long getUsableSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getUsableSpace();
    }

    public long getStoreUsedSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return FileUtils.sizeOf(this.dbPath);
    }

    public Endpoint getSelfEndpoint() {
        return this.storeOpts == null ? null : this.storeOpts.getServerAddress();
    }

    public RegionEngine getRegionEngine(final long regionId) {
        return this.regionEngineTable.get(regionId);
    }

    public List<RegionEngine> getAllRegionEngines() {
        return Lists.newArrayList(this.regionEngineTable.values());
    }

    public ExecutorService getReadIndexExecutor() {
        return readIndexExecutor;
    }

    public void setReadIndexExecutor(ExecutorService readIndexExecutor) {
        this.readIndexExecutor = readIndexExecutor;
    }

    public ExecutorService getRaftStateTrigger() {
        return raftStateTrigger;
    }

    public void setRaftStateTrigger(ExecutorService raftStateTrigger) {
        this.raftStateTrigger = raftStateTrigger;
    }

    public ExecutorService getSnapshotExecutor() {
        return snapshotExecutor;
    }

    public void setSnapshotExecutor(ExecutorService snapshotExecutor) {
        this.snapshotExecutor = snapshotExecutor;
    }

    public ExecutorService getCliRpcExecutor() {
        return cliRpcExecutor;
    }

    public void setCliRpcExecutor(ExecutorService cliRpcExecutor) {
        this.cliRpcExecutor = cliRpcExecutor;
    }

    public ExecutorService getRaftRpcExecutor() {
        return raftRpcExecutor;
    }

    public void setRaftRpcExecutor(ExecutorService raftRpcExecutor) {
        this.raftRpcExecutor = raftRpcExecutor;
    }

    public ExecutorService getKvRpcExecutor() {
        return kvRpcExecutor;
    }

    public void setKvRpcExecutor(ExecutorService kvRpcExecutor) {
        this.kvRpcExecutor = kvRpcExecutor;
    }

    public ScheduledExecutorService getMetricsScheduler() {
        return metricsScheduler;
    }

    public void setMetricsScheduler(ScheduledExecutorService metricsScheduler) {
        this.metricsScheduler = metricsScheduler;
    }

    public ScheduledReporter getKvMetricsReporter() {
        return kvMetricsReporter;
    }

    public void setKvMetricsReporter(ScheduledReporter kvMetricsReporter) {
        this.kvMetricsReporter = kvMetricsReporter;
    }

    public ScheduledReporter getThreadPoolMetricsReporter() {
        return threadPoolMetricsReporter;
    }

    public void setThreadPoolMetricsReporter(ScheduledReporter threadPoolMetricsReporter) {
        this.threadPoolMetricsReporter = threadPoolMetricsReporter;
    }

    /**
     * 将某个engine 从 映射表中移除 同时要终止该engine
     * @param regionId
     * @return
     */
    public boolean removeAndStopRegionEngine(final long regionId) {
        final RegionEngine engine = this.regionEngineTable.get(regionId);
        if (engine != null) {
            engine.shutdown();
            return true;
        }
        return false;
    }

    /**
     * 将所有地区中是 leader 的添加进列表中  那么 一个store 可能有多个leader 他们不属于一个 region
     * @return
     */
    public List<Long> getLeaderRegionIds() {
        final List<Long> regionIds = Lists.newArrayListWithCapacity(this.regionEngineTable.size());
        for (final RegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                regionIds.add(regionEngine.getRegion().getId());
            }
        }
        return regionIds;
    }

    public int getRegionCount() {
        return this.regionEngineTable.size();
    }

    public int getLeaderRegionCount() {
        int count = 0;
        for (final RegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                count++;
            }
        }
        return count;
    }

    /**
     * 判断是否繁忙???
     * @return
     */
    public boolean isBusy() {
        // Need more info
        return splitting.get();
    }

    /**
     * 进行拆分   看来是要将原来的某个region 拆分 那么数据怎么划分呢 还有region 与 node 之间到底是什么关系呢
     * @param regionId
     * @param newRegionId
     * @param closure
     */
    public void applySplit(final Long regionId, final Long newRegionId, final KVStoreClosure closure) {
        Requires.requireNonNull(regionId, "regionId");
        Requires.requireNonNull(newRegionId, "newRegionId");
        // 如果该region 已经存在就抛出异常
        if (this.regionEngineTable.containsKey(newRegionId)) {
            closure.setError(Errors.CONFLICT_REGION_ID);
            closure.run(new Status(-1, "Conflict region id %d", newRegionId));
            return;
        }
        // 设置成正在拆分中那么 对外显示就是繁忙状态
        if (!this.splitting.compareAndSet(false, true)) {
            closure.setError(Errors.SERVER_BUSY);
            closure.run(new Status(-1, "Server is busy now"));
            return;
        }
        // 获取对应的引擎 不存在 抛出异常
        final RegionEngine parentEngine = getRegionEngine(regionId);
        if (parentEngine == null) {
            closure.setError(Errors.NO_REGION_FOUND);
            closure.run(new Status(-1, "RegionEngine[%s] not found", regionId));
            this.splitting.set(false);
            return;
        }
        // 必须是 leader 才能进行拆分
        if (!parentEngine.isLeader()) {
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(-1, "RegionEngine[%s] not leader", regionId));
            this.splitting.set(false);
            return;
        }
        final Region parentRegion = parentEngine.getRegion();
        final byte[] startKey = BytesUtil.nullToEmpty(parentRegion.getStartKey());
        final byte[] endKey = parentRegion.getEndKey();
        // 拉取一定数量的key
        final long approximateKeys = this.rawKVStore.getApproximateKeysInRange(startKey, endKey);
        // 获取最少拆分的key
        final long leastKeysOnSplit = this.storeOpts.getLeastKeysOnSplit();
        if (approximateKeys < leastKeysOnSplit) {
            closure.setError(Errors.TOO_SMALL_TO_SPLIT);
            closure.run(new Status(-1, "RegionEngine[%s]'s keys less than %d", regionId, leastKeysOnSplit));
            this.splitting.set(false);
            return;
        }
        // 这里找到接近中间的位置
        final byte[] splitKey = this.rawKVStore.jumpOver(startKey, approximateKeys >> 1);
        if (splitKey == null) {
            closure.setError(Errors.STORAGE_ERROR);
            closure.run(new Status(-1, "Fail to scan split key"));
            this.splitting.set(false);
            return;
        }
        // 创建一个拆分操作 并交由 node 执行
        final KVOperation op = KVOperation.createRangeSplit(splitKey, regionId, newRegionId);
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        task.setDone(new KVClosureAdapter(closure, op));
        parentEngine.getNode().apply(task);
    }

    /**
     * 实际执行拆分的动作
     * @param regionId
     * @param newRegionId
     * @param splitKey
     */
    public void doSplit(final Long regionId, final Long newRegionId, final byte[] splitKey) {
        try {
            // 获取一些基本信息
            Requires.requireNonNull(regionId, "regionId");
            Requires.requireNonNull(newRegionId, "newRegionId");
            final RegionEngine parent = getRegionEngine(regionId);
            final Region region = parent.getRegion().copy();
            final RegionEngineOptions rOpts = parent.copyRegionOpts();
            region.setId(newRegionId);
            // 新的region 的startKey 为之前拆分的位置
            region.setStartKey(splitKey);
            region.setRegionEpoch(new RegionEpoch(-1, -1));

            rOpts.setRegionId(newRegionId);
            rOpts.setStartKeyBytes(region.getStartKey());
            rOpts.setEndKeyBytes(region.getEndKey());
            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), newRegionId));
            rOpts.setRaftDataPath(null);

            String baseRaftDataPath = this.storeOpts.getRaftDataPath();
            if (Strings.isBlank(baseRaftDataPath)) {
                baseRaftDataPath = "";
            }
            rOpts.setRaftDataPath(baseRaftDataPath + "raft_data_region_" + region.getId() + "_"
                                  + getSelfEndpoint().getPort());
            final RegionEngine engine = new RegionEngine(region, this);
            if (!engine.init(rOpts)) {
                LOG.error("Fail to init [RegionEngine: {}].", region);
                throw Errors.REGION_ENGINE_FAIL.exception();
            }

            // update parent conf  将新的region 添加到 映射中 (该region 实际是从旧的region分裂出来的)
            final Region pRegion = parent.getRegion();
            final RegionEpoch pEpoch = pRegion.getRegionEpoch();
            final long version = pEpoch.getVersion();
            pEpoch.setVersion(version + 1); // version + 1
            pRegion.setEndKey(splitKey); // update endKey

            // the following two lines of code can make a relation of 'happens-before' for
            // read 'pRegion', because that a write to a ConcurrentMap happens-before every
            // subsequent read of that ConcurrentMap.
            this.regionEngineTable.put(region.getId(), engine);
            registerRegionKVService(new DefaultRegionKVService(engine));

            // update local regionRouteTable
            this.pdClient.getRegionRouteTable().splitRegion(pRegion.getId(), region);
        } finally {
            this.splitting.set(false);
        }
    }

    private void startMetricReporters(final long metricsReportPeriod) {
        if (metricsReportPeriod <= 0) {
            return;
        }
        if (this.kvMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start kv store metrics reporter
            this.kvMetricsReporter = Slf4jReporter.forRegistry(KVMetrics.metricRegistry()) //
                .prefixedWith("store_" + this.storeId) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.kvMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
        if (this.threadPoolMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start threadPool metrics reporter
            this.threadPoolMetricsReporter = Slf4jReporter.forRegistry(MetricThreadPoolExecutor.metricRegistry()) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.threadPoolMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
    }

    /**
     * 初始化 KV 存储
     * @param opts
     * @return
     */
    private boolean initRawKVStore(final StoreEngineOptions opts) {
        final StorageType storageType = opts.getStorageType();
        switch (storageType) {
            case RocksDB:
                return initRocksDB(opts);
            case Memory:
                return initMemoryDB(opts);
            default:
                throw new UnsupportedOperationException("unsupported storage type: " + storageType);
        }
    }

    /**
     * 初始化 RocksDB
     * @param opts
     * @return
     */
    private boolean initRocksDB(final StoreEngineOptions opts) {
        RocksDBOptions rocksOpts = opts.getRocksDBOptions();
        if (rocksOpts == null) {
            // 创建一个默认opts
            rocksOpts = new RocksDBOptions();
            opts.setRocksDBOptions(rocksOpts);
        }
        String dbPath = rocksOpts.getDbPath();
        if (Strings.isNotBlank(dbPath)) {
            try {
                FileUtils.forceMkdir(new File(dbPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for dbPath {}.", dbPath);
                return false;
            }
        } else {
            // 默认为 ""
            dbPath = "";
        }
        final String childPath = "db_" + this.storeId + "_" + opts.getServerAddress().getPort();
        // 拼接路径后作为 DB 路径
        rocksOpts.setDbPath(Paths.get(dbPath, childPath).toString());
        // 创建db 文件
        this.dbPath = new File(rocksOpts.getDbPath());
        // 初始化基于 Rocks的 存储对象
        final RocksRawKVStore rocksRawKVStore = new RocksRawKVStore();
        if (!rocksRawKVStore.init(rocksOpts)) {
            LOG.error("Fail to init [RocksRawKVStore].");
            return false;
        }
        this.rawKVStore = rocksRawKVStore;
        return true;
    }

    private boolean initMemoryDB(final StoreEngineOptions opts) {
        MemoryDBOptions memoryOpts = opts.getMemoryDBOptions();
        if (memoryOpts == null) {
            memoryOpts = new MemoryDBOptions();
            opts.setMemoryDBOptions(memoryOpts);
        }
        final MemoryRawKVStore memoryRawKVStore = new MemoryRawKVStore();
        if (!memoryRawKVStore.init(memoryOpts)) {
            LOG.error("Fail to init [MemoryRawKVStore].");
            return false;
        }
        this.rawKVStore = memoryRawKVStore;
        return true;
    }

    /**
     * 初始化regionEngine
     * @param opts
     * @param store
     * @return
     */
    private boolean initAllRegionEngine(final StoreEngineOptions opts, final Store store) {
        Requires.requireNonNull(opts, "opts");
        Requires.requireNonNull(store, "store");
        // 默认的存放数据路径
        String baseRaftDataPath = opts.getRaftDataPath();
        if (Strings.isNotBlank(baseRaftDataPath)) {
            try {
                FileUtils.forceMkdir(new File(baseRaftDataPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for raftDataPath: {}.", baseRaftDataPath);
                return false;
            }
        } else {
            baseRaftDataPath = "";
        }
        final Endpoint serverAddress = opts.getServerAddress();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = store.getRegions();
        Requires.requireTrue(rOptsList.size() == regionList.size());
        for (int i = 0; i < rOptsList.size(); i++) {
            final RegionEngineOptions rOpts = rOptsList.get(i);
            final Region region = regionList.get(i);
            if (Strings.isBlank(rOpts.getRaftDataPath())) {
                // 生成一个默认的地址
                final String childPath = "raft_data_region_" + region.getId() + "_" + serverAddress.getPort();
                rOpts.setRaftDataPath(Paths.get(baseRaftDataPath, childPath).toString());
            }
            Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
            // 通过region 和本对象创建 regionEngine
            final RegionEngine engine = new RegionEngine(region, this);
            if (engine.init(rOpts)) {
                // 创建RegionKVService 相当于是一个入口 没有直接将 engine 暴露给外部
                final RegionKVService regionKVService = new DefaultRegionKVService(engine);
                // 将服务注册到storeEngine 上
                registerRegionKVService(regionKVService);
                this.regionEngineTable.put(region.getId(), engine);
            } else {
                LOG.error("Fail to init [RegionEngine: {}].", region);
                return false;
            }
        }
        return true;
    }

    /**
     * 将regionId 与对应的 服务 设置到映射容器中
     * @param regionKVService
     */
    private void registerRegionKVService(final RegionKVService regionKVService) {
        final RegionKVService preService = this.regionKVServiceTable.putIfAbsent(regionKVService.getRegionId(),
            regionKVService);
        if (preService != null) {
            throw new RheaRuntimeException("RegionKVService[region=" + regionKVService.getRegionId()
                                           + "] has already been registered, can not register again!");
        }
    }

    @Override
    public String toString() {
        return "StoreEngine{storeId=" + storeId + ", startTime=" + startTime + ", dbPath=" + dbPath + ", storeOpts="
               + storeOpts + ", started=" + started + '}';
    }
}
