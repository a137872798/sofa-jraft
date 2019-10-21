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

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.alipay.sofa.jraft.Closure;

/**
 * A thread-safe closure queue.
 * 一个线程安全的 回调队列
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-14 10:29:12 AM
 */
@ThreadSafe   // 该注解不具备任何功能 只是声明 该类应该要保证线程安全
public interface ClosureQueue {

    /**
     * Clear all closure in queue.
     * 清空回调队列
     */
    void clear();

    /**
     * Reset the first index in queue.
     * 重置队列的首个偏移量
     * @param firstIndex the first index of queue
     */
    void resetFirstIndex(final long firstIndex);

    /**
     * Append a new closure into queue.
     * 往队列中追加新的回调对象
     * @param closure the closure to append
     */
    void appendPendingClosure(final Closure closure);

    /**
     * Pop closure from queue until index(inclusion), returns the first
     * popped out index, returns -1 when out of range, returns index+1
     * when not found.
     * 从队列中弹出元素到传入的list 中 并返回弹出的元素个数
     * @param endIndex     the index of queue
     * @param closures     closure list
     * @return returns the first popped out index, returns -1 when out
     * of range, returns index+1
     * when not found.
     */
    long popClosureUntil(final long endIndex, final List<Closure> closures);

    /**
     * Pop closure from queue until index(inclusion), returns the first
     * popped out index, returns -1 when out of range, returns index+1
     * when not found.
     *
     * 弹出回调对象 如果实现了 TaskClosure 接口 那么 再往task列表中在添加一份   TaskClosure 同时监听 onCommited
     * @param endIndex     the index of queue
     * @param closures     closure list
     * @param taskClosures task closure list
     * @return returns the first popped out index, returns -1 when out
     * of range, returns index+1
     * when not found.
     */
    long popClosureUntil(final long endIndex, final List<Closure> closures, final List<TaskClosure> taskClosures);
}
