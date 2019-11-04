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
package com.alipay.sofa.jraft.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * 线程组工厂的默认实现
 * @author jiachun.fjc
 */
public final class DefaultFixedThreadsExecutorGroupFactory implements FixedThreadsExecutorGroupFactory {

    public static final DefaultFixedThreadsExecutorGroupFactory INSTANCE = new DefaultFixedThreadsExecutorGroupFactory();

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
                                                      final int maxPendingTasksPerThread) {
        return newExecutorGroup(nThreads, poolName, maxPendingTasksPerThread, false);
    }

    /**
     * 创建线程池工厂
     * @param nThreads
     * @param poolName
     * @param maxPendingTasksPerThread
     * @param useMpscQueue
     * @return
     */
    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
                                                      final int maxPendingTasksPerThread, final boolean useMpscQueue) {
        Requires.requireTrue(nThreads > 0, "nThreads must > 0");
        //  是否使用 mpsc 队列
        final boolean mpsc = useMpscQueue && Utils.USE_MPSC_SINGLE_THREAD_EXECUTOR;
        final SingleThreadExecutor[] children = new SingleThreadExecutor[nThreads];
        final ThreadFactory threadFactory = mpsc ? new NamedThreadFactory(poolName, true) : null;
        for (int i = 0; i < nThreads; i++) {
            if (mpsc) {
                // 内部使用了mpsc 队列
                children[i] = new MpscSingleThreadExecutor(maxPendingTasksPerThread, threadFactory);
            } else {
                // 普通的单线程执行器
                children[i] = new DefaultSingleThreadExecutor(poolName, maxPendingTasksPerThread);
            }
        }
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    /**
     * 如果传入一组 单线程执行器 包装成一个线程组返回
     * @param children
     * @return
     */
    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children) {
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    /**
     * 内部还有选择器
     * @param children
     * @param chooser
     * @return
     */
    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children,
                                                      final ExecutorChooserFactory.ExecutorChooser chooser) {
        return new DefaultFixedThreadsExecutorGroup(children, chooser);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children) {
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children,
                                                      final ExecutorChooserFactory.ExecutorChooser chooser) {
        return new DefaultFixedThreadsExecutorGroup(children, chooser);
    }

    private DefaultFixedThreadsExecutorGroupFactory() {
    }
}
