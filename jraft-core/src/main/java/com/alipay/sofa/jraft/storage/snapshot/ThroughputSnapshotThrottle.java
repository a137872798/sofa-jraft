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
package com.alipay.sofa.jraft.storage.snapshot;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.Utils;

/**
 * SnapshotThrottle with throughput threshold used in installSnapshot.
 * 吞吐量 阀门对象
 * 可以理解为一种限流桶算法吧
 *
 * @author dennis
 */
public class ThroughputSnapshotThrottle implements SnapshotThrottle {

    /**
     * 推测是每次允许的吞吐量
     */
    private final long throttleThroughputBytes;
    /**
     * 多少秒重置一次throttleThroughputBytes
     */
    private final long checkCycleSecs;
    /**
     * 上一次检查时间戳
     */
    private long       lastThroughputCheckTimeUs;
    /**
     * 当前吞吐量大小 该值是啥意思???
     */
    private long       currThroughputBytes;
    private final Lock lock = new ReentrantLock();
    /**
     * 基础校准时间???
     */
    private final long baseAligningTimeUs;

    public ThroughputSnapshotThrottle(final long throttleThroughputBytes, final long checkCycleSecs) {
        this.throttleThroughputBytes = throttleThroughputBytes;
        this.checkCycleSecs = checkCycleSecs;
        this.currThroughputBytes = 0L;
        // 啥意思???
        this.baseAligningTimeUs = 1000 * 1000 / checkCycleSecs;
        this.lastThroughputCheckTimeUs = this.calculateCheckTimeUs(Utils.monotonicUs());
    }

    /**
     * 计算检查的时间
     * @param currTimeUs
     * @return
     */
    private long calculateCheckTimeUs(final long currTimeUs) {
        return currTimeUs / this.baseAligningTimeUs * this.baseAligningTimeUs;
    }

    /**
     * 入参是本次拉取的最大长度  返回值是处理后的长度
     * @param bytes expect size
     * @return
     */
    @Override
    public long throttledByThroughput(final long bytes) {
        long availableSize;
        final long nowUs = Utils.monotonicUs();
        // 每次循环最多允许的量
        final long limitPerCycle = this.throttleThroughputBytes / this.checkCycleSecs;
        this.lock.lock();
        try {
            // 如果当前累计的吞吐量加上本次 传入的值 超过了限定值
            if (this.currThroughputBytes + bytes > limitPerCycle) {
                // reading another |bytes| exceeds the limit
                // 未到一个校验基准时间
                if (nowUs - this.lastThroughputCheckTimeUs <= 1000 * 1000 / this.checkCycleSecs) {
                    // if time interval is less than or equal to a cycle, read more data
                    // to make full use of the throughput of current cycle.
                    // 代表本次允许传输的量  同时currThroughputBytes 达到了最大限定值
                    availableSize = limitPerCycle - this.currThroughputBytes;
                    this.currThroughputBytes = limitPerCycle;
                } else {
                    // otherwise, read the data in the next cycle.
                    // 这啥意思???  只要本次读取数据没到最大值 就允许全部读取
                    availableSize = bytes > limitPerCycle ? limitPerCycle : bytes;
                    this.currThroughputBytes = availableSize;
                    this.lastThroughputCheckTimeUs = calculateCheckTimeUs(nowUs);
                }
            } else {
                // reading another |bytes| doesn't exceed limit(less than or equal to),
                // put it in current cycle
                // 每超过限定值就是最简单的情况
                availableSize = bytes;
                this.currThroughputBytes += availableSize;
            }
        } finally {
            this.lock.unlock();
        }
        return availableSize;
    }
}
