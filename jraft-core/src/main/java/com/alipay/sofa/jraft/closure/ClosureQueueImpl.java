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
package com.alipay.sofa.jraft.closure;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Closure queue implementation.
 * 回调对象队列实现类  可以通过优化并发模型来提升性能吗 在并发数高的情况 性能会下降
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 11:44:01 AM
 */
public class ClosureQueueImpl implements ClosureQueue {

    private static final Logger LOG = LoggerFactory.getLogger(ClosureQueueImpl.class);

    /**
     * 看来是用锁对象实现队列线程安全的
     */
    private final Lock          lock;
    /**
     * 维护了首个下标
     */
    private long                firstIndex;
    /**
     * 队列使用了链表结构
     */
    private LinkedList<Closure> queue;

    @OnlyForTest
    public long getFirstIndex() {
        return firstIndex;
    }

    @OnlyForTest
    public LinkedList<Closure> getQueue() {
        return queue;
    }

    public ClosureQueueImpl() {
        super();
        this.lock = new ReentrantLock();
        this.firstIndex = 0;
        this.queue = new LinkedList<>();
    }

    /**
     * 清空队列 并触发所有回调(以关闭状态)
     */
    @Override
    public void clear() {
        List<Closure> savedQueue;
        this.lock.lock();
        try {
            // 在锁中 将queue 指向一个新对象 相当于也是减小锁力度 (将可能会耗时的对象方法在外部处理)
            this.firstIndex = 0;
            savedQueue = this.queue;
            this.queue = new LinkedList<>();
        } finally {
            this.lock.unlock();
        }

        // 清空这里使用的 状态码为 Leader 已关闭 看来该方法的调用 一般是在某个 Leader 下线的时候
        final Status status = new Status(RaftError.EPERM, "Leader stepped down");
        for (final Closure done : savedQueue) {
            if (done != null) {
                Utils.runClosureInThread(done, status);
            }
        }
    }

    /**
     * 重置 first 的值
     * @param firstIndex the first index of queue
     */
    @Override
    public void resetFirstIndex(final long firstIndex) {
        this.lock.lock();
        try {
            Requires.requireTrue(this.queue.isEmpty(), "Queue is not empty.");
            this.firstIndex = firstIndex;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 增加闲置的 Closure
     * @param closure the closure to append
     */
    @Override
    public void appendPendingClosure(final Closure closure) {
        this.lock.lock();
        try {
            this.queue.add(closure);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long popClosureUntil(final long endIndex, final List<Closure> closures) {
        return popClosureUntil(endIndex, closures, null);
    }

    /**
     * 弹出指定数量的回调对象   返回 endIndex +1 代表没找到 -1 代表越界 默认返回 首个弹出的下标(也就是firstIndex)
     * @param endIndex     the index of queue
     * @param closures     closure list
     * @param taskClosures task closure list
     * @return
     */
    @Override
    public long popClosureUntil(final long endIndex, final List<Closure> closures, final List<TaskClosure> taskClosures) {
        // 首先将task 列表元素清空
        closures.clear();
        if (taskClosures != null) {
            taskClosures.clear();
        }
        this.lock.lock();
        try {
            final int queueSize = this.queue.size();
            if (queueSize == 0 || endIndex < this.firstIndex) {
                return endIndex + 1;
            }
            if (endIndex > this.firstIndex + queueSize - 1) {
                LOG.error("Invalid endIndex={}, firstIndex={}, closureQueueSize={}", endIndex, this.firstIndex,
                    queueSize);
                return -1;
            }
            final long outFirstIndex = this.firstIndex;
            for (long i = outFirstIndex; i <= endIndex; i++) {
                final Closure closure = this.queue.pollFirst();
                if (taskClosures != null && closure instanceof TaskClosure) {
                    taskClosures.add((TaskClosure) closure);
                }
                closures.add(closure);
            }
            this.firstIndex = endIndex + 1;
            return outFirstIndex;
        } finally {
            this.lock.unlock();
        }
    }
}
