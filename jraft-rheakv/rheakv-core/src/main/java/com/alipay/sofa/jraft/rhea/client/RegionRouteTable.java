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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Region routing table.
 *
 * Enter a 'key' or a 'key range', which can calculate the region
 * in which the 'key' is located, and can also calculate all
 * regions of a 'key range' hit.
 *
 * 如果 pd 服务 可用 会从pd server 刷新 路由信息 否则从本地配置刷新信息
 * If the pd server is enabled, the routing data will be refreshed
 * from the pd server, otherwise the routing data is completely
 * based on the local configuration.
 *
 * <pre>
 *
 *                                         ┌───────────┐
 *                                         │ input key │
 *                                         └─────┬─────┘
 *                                               │
 *                                               │
 *                                               │
 * ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─        ┌ ─ ─ ─ ─ ─ ─ ┐    │      ┌ ─ ─ ─ ─ ─ ─ ┐           ┌ ─ ─ ─ ─ ─ ─ ┐
 *  startKey1=byte[0] │          startKey2       │         startKey3                 startKey4
 * └ ─ ─ ─ ┬ ─ ─ ─ ─ ─        └ ─ ─ ─│─ ─ ─ ┘    │      └ ─ ─ ─│─ ─ ─ ┘           └ ─ ─ ─│─ ─ ─ ┘
 *         │                         │           │             │                         │
 *         ▼─────────────────────────▼───────────▼─────────────▼─────────────────────────▼─────────────────────────┐
 *         │                         │                         │                         │                         │
 *         │                         │                         │                         │                         │
 *         │         region1         │         region2         │          region3        │         region4         │
 *         │                         │                         │                         │                         │
 *         └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
 *
 * </pre>
 *
 * You can seen that the most suitable data structure for implementing the
 * above figure is a skip list or a binary tree (for the closest matches for
 * given search).
 *
 * In addition, selecting the startKey or endKey of the region as the key of
 * the RegionRouteTable is also exquisite.
 *
 * For example, why not use endKey?
 * This depends mainly on the way the region splits:
 *  a) Suppose that region2[startKey2, endKey2) with id 2 is split
 *  b) The two regions after splitting are region2[startKey2, splitKey) with
 *      id continuing to 2 and region3[splitKey, endKey2) with id 3.
 *  c) At this point, you only need to add an element <region3, splitKey> to
 *      the RegionRouteTable. The data of region2 does not need to be modified.
 * 区域路由表
 * @author jiachun.fjc
 */
public class RegionRouteTable {

    private static final Logger              LOG                = LoggerFactory.getLogger(RegionRouteTable.class);

    /**
     * 比较 一个byte[] 的 函数
     */
    private static final Comparator<byte[]>  keyBytesComparator = BytesUtil.getDefaultByteArrayComparator();

    /**
     * 读写锁
     */
    private final StampedLock                stampedLock        = new StampedLock();
    /**
     * key: startKey[]  value: regionId
     */
    private final NavigableMap<byte[], Long> rangeTable         = new TreeMap<>(keyBytesComparator);
    private final Map<Long, Region>          regionTable        = Maps.newHashMap();

    /**
     * 通过 regionId 获取 region
     * @param regionId
     * @return
     */
    public Region getRegionById(final long regionId) {
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        // validate() emit a load-fence, but no store-fence.  So you should only have
        // load instructions inside a block of tryOptimisticRead() / validate(),
        // because it is meant to the a read-only operation, and therefore, it is fine
        // to use the loadFence() function to avoid re-ordering.
        // 从map中获取 并通过深拷贝的方式复制数据
        Region region = safeCopy(this.regionTable.get(regionId));
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                region = safeCopy(this.regionTable.get(regionId));
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return region;
    }

    /**
     * 更新或 首次添加region 到map 中   每个 region 都对应一个 jraft组
     * @param region
     */
    public void addOrUpdateRegion(final Region region) {
        Requires.requireNonNull(region, "region");
        Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
        // 获得本地区id
        final long regionId = region.getId();
        // 获取开始的 key 数组
        final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            // 在初始化 PD时 会维护 regionId 与 region的信息  以及 startKey 与regionId的信息
            this.regionTable.put(regionId, region.copy());
            // 这里使用了红黑树来保存regionId
            this.rangeTable.put(startKey, regionId);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }


    /**
     * 拆分地区
     * @param leftId  对应某个region 的 startKey
     * @param right
     */
    public void splitRegion(final long leftId, final Region right) {
        Requires.requireNonNull(right, "right");
        Requires.requireNonNull(right.getRegionEpoch(), "right.regionEpoch");
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            final Region left = this.regionTable.get(leftId);
            Requires.requireNonNull(left, "left");
            // 获取2个region的 左右 key[]
            final byte[] leftStartKey = BytesUtil.nullToEmpty(left.getStartKey());
            final byte[] leftEndKey = left.getEndKey();
            final long rightId = right.getId();
            final byte[] rightStartKey = right.getStartKey();
            final byte[] rightEndKey = right.getEndKey();
            Requires.requireNonNull(rightStartKey, "rightStartKey");
            Requires.requireTrue(BytesUtil.compare(leftStartKey, rightStartKey) < 0,
                "leftStartKey must < rightStartKey");
            // 这里的意思是 右侧 region 的 end 必须和左侧一致
            if (leftEndKey == null || rightEndKey == null) {
                Requires.requireTrue(leftEndKey == rightEndKey, "leftEndKey must == rightEndKey");
            } else {
                Requires.requireTrue(BytesUtil.compare(leftEndKey, rightEndKey) == 0, "leftEndKey must == rightEndKey");
                Requires.requireTrue(BytesUtil.compare(rightStartKey, rightEndKey) < 0,
                    "rightStartKey must < rightEndKey");
            }
            // 看作是 region 的一个状态描述对象
            final RegionEpoch leftEpoch = left.getRegionEpoch();
            // 当region 分裂或者 融合的时候 会将version+1
            leftEpoch.setVersion(leftEpoch.getVersion() + 1);
            // 将左边的region 的终止位置更新为 right 的startKey
            left.setEndKey(rightStartKey);
            this.regionTable.put(rightId, right.copy());
            this.rangeTable.put(rightStartKey, rightId);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * 从regionTable 和 rangeTable 中移除数据
     * @param regionId
     * @return
     */
    public boolean removeRegion(final long regionId) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            final Region region = this.regionTable.remove(regionId);
            if (region != null) {
                final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
                return this.rangeTable.remove(startKey) != null;
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return false;
    }

    /**
     * Returns the region to which the key belongs.
     */
    public Region findRegionByKey(final byte[] key) {
        Requires.requireNonNull(key, "key");
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            return findRegionByKeyWithoutLock(key);
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * 根据key 找到region id 之后去 regionTable 找到region
     * @param key
     * @return
     */
    private Region findRegionByKeyWithoutLock(final byte[] key) {
        // return the greatest key less than or equal to the given key
        final Map.Entry<byte[], Long> entry = this.rangeTable.floorEntry(key);
        if (entry == null) {
            reportFail(key);
            throw reject(key, "fail to find region by key");
        }
        return this.regionTable.get(entry.getValue());
    }

    /**
     * Returns the list of regions to which the keys belongs.
     * 将key 对应的region 作为 返回map的 key
     */
    public Map<Region, List<byte[]>> findRegionsByKeys(final List<byte[]> keys) {
        Requires.requireNonNull(keys, "keys");
        final Map<Region, List<byte[]>> regionMap = Maps.newHashMap();
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            for (final byte[] key : keys) {
                final Region region = findRegionByKeyWithoutLock(key);
                regionMap.computeIfAbsent(region, k -> Lists.newArrayList()).add(key);
            }
            return regionMap;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the list of regions to which the keys belongs.
     * 看来 KVEntry 中的 key[] 就等同于 region的 startKey[]
     */
    public Map<Region, List<KVEntry>> findRegionsByKvEntries(final List<KVEntry> kvEntries) {
        Requires.requireNonNull(kvEntries, "kvEntries");
        final Map<Region, List<KVEntry>> regionMap = Maps.newHashMap();
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            for (final KVEntry kvEntry : kvEntries) {
                final Region region = findRegionByKeyWithoutLock(kvEntry.getKey());
                regionMap.computeIfAbsent(region, k -> Lists.newArrayList()).add(kvEntry);
            }
            return regionMap;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the list of regions covered by startKey and endKey.
     * 根据范围锁定一组region    某个region的 endKey[] 实际上对应另一个 region的startKey[]
     */
    public List<Region> findRegionsByKeyRange(final byte[] startKey, final byte[] endKey) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
            final NavigableMap<byte[], Long> subRegionMap;
            // 下面2个方法都没有包含startKey 对应的 region
            if (endKey == null) {
                // 代表返回一颗子树
                subRegionMap = this.rangeTable.tailMap(realStartKey, false);
            } else {
                subRegionMap = this.rangeTable.subMap(realStartKey, false, endKey, true);
            }
            final List<Region> regionList = Lists.newArrayListWithCapacity(subRegionMap.size() + 1);
            // 找到 startKey 对应的region
            final Map.Entry<byte[], Long> headEntry = this.rangeTable.floorEntry(realStartKey);
            if (headEntry == null) {
                reportFail(startKey);
                throw reject(startKey, "fail to find region by startKey");
            }
            regionList.add(safeCopy(this.regionTable.get(headEntry.getValue())));
            for (final Long regionId : subRegionMap.values()) {
                regionList.add(safeCopy(this.regionTable.get(regionId)));
            }
            return regionList;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the startKey of next region.
     */
    public byte[] findStartKeyOfNextRegion(final byte[] key) {
        Requires.requireNonNull(key, "key");
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        // get the least key strictly greater than the given key
        // 找到 比传入key 大的 下个 region的 startkey
        byte[] nextStartKey = this.rangeTable.higherKey(key);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                // get the least key strictly greater than the given key
                nextStartKey = this.rangeTable.higherKey(key);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return nextStartKey;
    }

    // Should be in lock
    //
    // If this method is called, either because the registered region table is incomplete (by user)
    // or because of a bug.
    private void reportFail(final byte[] relatedKey) {
        if (LOG.isErrorEnabled()) {
            LOG.error("There is a high probability that the data in the region table is corrupted.");
            LOG.error("---------------------------------------------------------------------------");
            LOG.error("* RelatedKey:  {}.", BytesUtil.toHex(relatedKey));
            LOG.error("* RangeTable:  {}.", this.rangeTable);
            LOG.error("* RegionTable: {}.", this.regionTable);
            LOG.error("---------------------------------------------------------------------------");
        }
    }

    private static Region safeCopy(final Region region) {
        if (region == null) {
            return null;
        }
        return region.copy();
    }

    private static RouteTableException reject(final byte[] relatedKey, final String message) {
        return new RouteTableException("key: " + BytesUtil.toHex(relatedKey) + ", message: " + message);
    }
}
