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

import java.util.ArrayDeque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.util.BytesUtil;

/**
 * Rhea 迭代器
 * @author jiachun.fjc
 */
public class DefaultRheaIterator implements RheaIterator<KVEntry> {

    /**
     * kv 存储仓库
     */
    private final DefaultRheaKVStore    rheaKVStore;
    /**
     * 内部包含region 信息
     */
    private final PlacementDriverClient pdClient;
    private final byte[]                startKey;
    private final byte[]                endKey;
    /**
     * 设置该标识 会需要先调用readIndex  成功后才允许获取数据
     */
    private final boolean               readOnlySafe;
    private final boolean               returnValue;
    private final int                   bufSize;
    /**
     * 迭代器中存在的数据
     */
    private final Queue<KVEntry>        buf;

    /**
     * 当前指向的key
     */
    private byte[]                      cursorKey;

    public DefaultRheaIterator(DefaultRheaKVStore rheaKVStore, byte[] startKey, byte[] endKey, int bufSize,
                               boolean readOnlySafe, boolean returnValue) {
        this.rheaKVStore = rheaKVStore;
        this.pdClient = rheaKVStore.getPlacementDriverClient();
        this.startKey = BytesUtil.nullToEmpty(startKey);
        this.endKey = endKey;
        this.bufSize = bufSize;
        this.readOnlySafe = readOnlySafe;
        this.returnValue = returnValue;
        this.buf = new ArrayDeque<>(bufSize);
        this.cursorKey = this.startKey;
    }

    /**
     * 判断是否有下个元素
     * @return
     */
    @Override
    public synchronized boolean hasNext() {
        if (this.buf.isEmpty()) {
            while (this.endKey == null || BytesUtil.compare(this.cursorKey, this.endKey) < 0) {
                // 扫描单个 region 并返回一组KVEntry
                final List<KVEntry> kvEntries = this.rheaKVStore.singleRegionScan(this.cursorKey, this.endKey,
                    this.bufSize, this.readOnlySafe, this.returnValue);
                if (kvEntries.isEmpty()) {
                    // cursorKey jump to next region's startKey   如果找不到数据就去下个 region 查找
                    this.cursorKey = this.pdClient.findStartKeyOfNextRegion(this.cursorKey, false);
                    // 代表之后没有数据了
                    if (cursorKey == null) { // current is the last region
                        break;
                    }
                } else {
                    // 将拉取的数据全部存入 buf
                    final KVEntry last = kvEntries.get(kvEntries.size() - 1);
                    this.cursorKey = BytesUtil.nextBytes(last.getKey()); // cursorKey++
                    this.buf.addAll(kvEntries);
                    break;
                }
            }
            return !this.buf.isEmpty();
        }
        return true;
    }

    /**
     * 从buf 中拉取下个元素
     * @return
     */
    @Override
    public synchronized KVEntry next() {
        if (this.buf.isEmpty()) {
            throw new NoSuchElementException();
        }
        return this.buf.poll();
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public int getBufSize() {
        return bufSize;
    }
}
