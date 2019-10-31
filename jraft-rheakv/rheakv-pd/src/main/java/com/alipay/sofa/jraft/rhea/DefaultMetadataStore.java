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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.metadata.StoreStats;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.LongSequence;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * 默认元数据存储对象
 * @author jiachun.fjc
 */
public class DefaultMetadataStore implements MetadataStore {

    private static final Logger                       LOG                  = LoggerFactory
                                                                               .getLogger(DefaultMetadataStore.class);

    private final ConcurrentMap<String, LongSequence> storeSequenceMap     = Maps.newConcurrentMap();
    private final ConcurrentMap<String, LongSequence> regionSequenceMap    = Maps.newConcurrentMap();
    /**
     * key clusterId  value  storeIdSet
     */
    private final ConcurrentMap<Long, Set<Long>>      clusterStoreIdsCache = Maps.newConcurrentMapLong();
    private final Serializer                          serializer           = Serializers.getDefault();
    /**
     * 内部包含一个 store 对象 该对象用于协调内部的多个组件
     */
    private final RheaKVStore                         rheaKVStore;

    public DefaultMetadataStore(RheaKVStore rheaKVStore) {
        this.rheaKVStore = rheaKVStore;
    }

    /**
     * 通过 集群id 查询集群对象
     * @param clusterId
     * @return
     */
    @Override
    public Cluster getClusterInfo(final long clusterId) {
        // 从 映射容器中找到一组对应的storeId
        final Set<Long> storeIds = getClusterIndex(clusterId);
        if (storeIds == null) {
            return null;
        }
        final List<byte[]> storeKeys = Lists.newArrayList();
        for (final Long storeId : storeIds) {
            // 集群id 和storeId 组成了 storeKey
            final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
            storeKeys.add(BytesUtil.writeUtf8(storeInfoKey));
        }
        // 通过kvstore 对象 将对应的数据拉取出来  最终就是委托到 RocksDB 对象 该对象本身就具备批量拉取数据的能力
        final Map<ByteArray, byte[]> storeInfoBytes = this.rheaKVStore.bMultiGet(storeKeys);
        final List<Store> stores = Lists.newArrayListWithCapacity(storeInfoBytes.size());
        for (final byte[] storeBytes : storeInfoBytes.values()) {
            // 将数据反序列化成store  ByteArray 就是 将key[] 包装后的对象  同时在RocksDB 中 key value 都是byte[]
            final Store store = this.serializer.readObject(storeBytes, Store.class);
            stores.add(store);
        }
        return new Cluster(clusterId, stores);
    }

    /**
     * 根据 集群id 和 地址构建 key 并去db 对象中找到storeId  (key 对应的值就是 storeId)
     * @param clusterId
     * @param endpoint
     * @return
     */
    @Override
    public Long getOrCreateStoreId(final long clusterId, final Endpoint endpoint) {
        // 生成格式化数据
        final String storeIdKey = MetadataKeyHelper.getStoreIdKey(clusterId, endpoint);
        // 获取对应的value
        final byte[] bytesVal = this.rheaKVStore.bGet(storeIdKey);
        // 代表需要创建一个store 对象
        if (bytesVal == null) {
            final String storeSeqKey = MetadataKeyHelper.getStoreSeqKey(clusterId);
            // 记录当前store 对应的序列值
            LongSequence storeSequence = this.storeSequenceMap.get(storeSeqKey);
            if (storeSequence == null) {
                final LongSequence newStoreSequence = new LongSequence() {

                    @Override
                    public Sequence getNextSequence() {
                        // 这里将 key 原来对应的位置向后推移了 32位 并将 原来的值 和延后 的值包装成一个Sequence 并返回
                        return rheaKVStore.bGetSequence(storeSeqKey, 32);
                    }
                };
                // 将当前序列设置进去
                storeSequence = this.storeSequenceMap.putIfAbsent(storeSeqKey, newStoreSequence);
                if (storeSequence == null) {
                    storeSequence = newStoreSequence;
                }
            }
            // 首次调用 会通过 getNextSequence 获取序列值
            final long newStoreId = storeSequence.next();
            final byte[] newBytesVal = new byte[8];
            Bits.putLong(newBytesVal, 0, newStoreId);
            // 将数据保存到 db 中
            final byte[] oldBytesVal = this.rheaKVStore.bPutIfAbsent(storeIdKey, newBytesVal);
            if (oldBytesVal != null) {
                return Bits.getLong(oldBytesVal, 0);
            } else {
                // 该值代表新的storeId 对象
                return newStoreId;
            }
        }
        return Bits.getLong(bytesVal, 0);
    }

    /**
     * 获取 store 信息   clusterId 和 storeId 去 DB 上获取 就是 store了 如果是 clusterId 和 endpoint 就是获取到 storeId
     * @param clusterId
     * @param storeId
     * @return
     */
    @Override
    public Store getStoreInfo(final long clusterId, final long storeId) {
        final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
        final byte[] bytes = this.rheaKVStore.bGet(storeInfoKey);
        if (bytes == null) {
            Store empty = new Store();
            empty.setId(storeId);
            return empty;
        }
        return this.serializer.readObject(bytes, Store.class);
    }

    @Override
    public Store getStoreInfo(final long clusterId, final Endpoint endpoint) {
        final long storeId = getOrCreateStoreId(clusterId, endpoint);
        return getStoreInfo(clusterId, storeId);
    }

    /**
     * 更新 store 信息
     * @param clusterId
     * @param store
     * @return
     */
    @Override
    public CompletableFuture<Store> updateStoreInfo(final long clusterId, final Store store) {
        final long storeId = store.getId();
        final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
        final byte[] bytes = this.serializer.writeObject(store);
        final CompletableFuture<Store> future = new CompletableFuture<>();
        // 将数据写入到 db 后 触发
        this.rheaKVStore.getAndPut(storeInfoKey, bytes).whenComplete((prevBytes, getPutThrowable) -> {
            if (getPutThrowable == null) {
                if (prevBytes != null) {
                    // 正常插入 如果之前已经存在过某个 store 将该对象存入到future 中
                    future.complete(serializer.readObject(prevBytes, Store.class));
                } else {
                    // 代表该store 是新插入的 那么将 storeId 追加到 clusterId 对应的数据内
                    mergeClusterIndex(clusterId, storeId).whenComplete((ignored, mergeThrowable) -> {
                        if (mergeThrowable == null) {
                            future.complete(null);
                        } else {
                            future.completeExceptionally(mergeThrowable);
                        }
                    });
                }
            } else {
                future.completeExceptionally(getPutThrowable);
            }
        });
        return future;
    }


    @Override
    public Long createRegionId(final long clusterId) {
        // 生成查询 region 的key
        final String regionSeqKey = MetadataKeyHelper.getRegionSeqKey(clusterId);
        // 获取 region 序列
        LongSequence regionSequence = this.regionSequenceMap.get(regionSeqKey);
        if (regionSequence == null) {
            final LongSequence newRegionSequence = new LongSequence(Region.MAX_ID_WITH_MANUAL_CONF) {

                // 通过key 获取到 偏移量 并 继续推进32 返回 起始值 和终点 作为 sequence
                @Override
                public Sequence getNextSequence() {
                    return rheaKVStore.bGetSequence(regionSeqKey, 32);
                }
            };
            regionSequence = this.regionSequenceMap.putIfAbsent(regionSeqKey, newRegionSequence);
            if (regionSequence == null) {
                regionSequence = newRegionSequence;
            }
        }
        return regionSequence.next();
    }

    @Override
    public StoreStats getStoreStats(final long clusterId, final long storeId) {
        final String key = MetadataKeyHelper.getStoreStatsKey(clusterId, storeId);
        final byte[] bytes = this.rheaKVStore.bGet(key);
        if (bytes == null) {
            return null;
        }
        return this.serializer.readObject(bytes, StoreStats.class);
    }

    @Override
    public CompletableFuture<Boolean> updateStoreStats(final long clusterId, final StoreStats storeStats) {
        final String key = MetadataKeyHelper.getStoreStatsKey(clusterId, storeStats.getStoreId());
        final byte[] bytes = this.serializer.writeObject(storeStats);
        return this.rheaKVStore.put(key, bytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<Region, RegionStats> getRegionStats(final long clusterId, final Region region) {
        final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, region.getId());
        final byte[] bytes = this.rheaKVStore.bGet(key);
        if (bytes == null) {
            return null;
        }
        return this.serializer.readObject(bytes, Pair.class);
    }

    @Override
    public CompletableFuture<Boolean> updateRegionStats(final long clusterId, final Region region,
                                                        final RegionStats regionStats) {
        final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, region.getId());
        final byte[] bytes = this.serializer.writeObject(Pair.of(region, regionStats));
        return this.rheaKVStore.put(key, bytes);
    }

    @Override
    public CompletableFuture<Boolean> batchUpdateRegionStats(final long clusterId,
                                                             final List<Pair<Region, RegionStats>> regionStatsList) {
        final List<KVEntry> entries = Lists.newArrayListWithCapacity(regionStatsList.size());
        for (final Pair<Region, RegionStats> p : regionStatsList) {
            final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, p.getKey().getId());
            final byte[] bytes = this.serializer.writeObject(p);
            entries.add(new KVEntry(BytesUtil.writeUtf8(key), bytes));
        }
        return this.rheaKVStore.put(entries);
    }

    /**
     * 通过集群id 查询 storeIds
     * @param clusterId
     * @return
     */
    @Override
    public Set<Long> unsafeGetStoreIds(final long clusterId) {
        Set<Long> storeIds = this.clusterStoreIdsCache.get(clusterId);
        if (storeIds != null) {
            return storeIds;
        }
        storeIds = getClusterIndex(clusterId);
        this.clusterStoreIdsCache.put(clusterId, storeIds);
        return storeIds;
    }

    /**
     * 通过端点 查询 storeId
     * @param clusterId
     * @param endpoints
     * @return
     */
    @Override
    public Map<Long, Endpoint> unsafeGetStoreIdsByEndpoints(final long clusterId, final List<Endpoint> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return Collections.emptyMap();
        }
        final List<byte[]> storeIdKeyList = Lists.newArrayListWithCapacity(endpoints.size());
        final Map<ByteArray, Endpoint> keyToEndpointMap = Maps.newHashMapWithExpectedSize(endpoints.size());
        for (final Endpoint endpoint : endpoints) {
            final byte[] keyBytes = BytesUtil.writeUtf8(MetadataKeyHelper.getStoreIdKey(clusterId, endpoint));
            storeIdKeyList.add(keyBytes);
            keyToEndpointMap.put(ByteArray.wrap(keyBytes), endpoint);
        }
        final Map<ByteArray, byte[]> storeIdBytes = this.rheaKVStore.bMultiGet(storeIdKeyList);
        final Map<Long, Endpoint> ids = Maps.newHashMapWithExpectedSize(storeIdBytes.size());
        for (final Map.Entry<ByteArray, byte[]> entry : storeIdBytes.entrySet()) {
            final Long storeId = Bits.getLong(entry.getValue(), 0);
            final Endpoint endpoint = keyToEndpointMap.get(entry.getKey());
            ids.put(storeId, endpoint);
        }
        return ids;
    }

    @Override
    public void invalidCache() {
        this.clusterStoreIdsCache.clear();
    }

    /**
     * 通过集群 id 查询storeId
     * @param clusterId
     * @return
     */
    private Set<Long> getClusterIndex(final long clusterId) {
        final String key = MetadataKeyHelper.getClusterInfoKey(clusterId);
        // 看来 cluster 对应的 value是多个 storeId 以 "," 拼接的字符串
        final byte[] indexBytes = this.rheaKVStore.bGet(key);
        if (indexBytes == null) {
            return null;
        }
        final String strVal = BytesUtil.readUtf8(indexBytes);
        final String[] array = Strings.split(strVal, ',');
        if (array == null) {
            return null;
        }
        final Set<Long> storeIds = new HashSet<>(array.length);
        for (final String str : array) {
            storeIds.add(Long.parseLong(str.trim()));
        }
        return storeIds;
    }

    private CompletableFuture<Boolean> mergeClusterIndex(final long clusterId, final long storeId) {
        // 生成获取 cluster 信息的 key
        final String key = MetadataKeyHelper.getClusterInfoKey(clusterId);
        // 应该是将 storeId 加入到该cluster 中
        final CompletableFuture<Boolean> future = this.rheaKVStore.merge(key, String.valueOf(storeId));
        future.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                LOG.error("Fail to merge cluster index, {}, {}.", key, StackTraceUtil.stackTrace(throwable));
            }
            // 因为插入了新数据 缓存已经失效了 但是换句话说 这里还是有一定的延时的可能会读到旧数据
            // 如果反过来先删除缓存 那么 可能在merge前 又读了 旧数据 那么之后也不会再删除旧缓存了
            clusterStoreIdsCache.clear();
        });
        return future;
    }
}
