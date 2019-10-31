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
package com.alipay.sofa.jraft.rhea.util.pipeline.future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.util.Maps;

/**
 * 该对象 加强了 CompletableFuture 代表可以给一个异步结果提前设置各种加工逻辑
 * @author jiachun.fjc
 */
public class DefaultPipelineFuture<V> extends CompletableFuture<V> implements PipelineFuture<V> {

    private static final Logger                                        LOG                         = LoggerFactory
                                                                                                       .getLogger(DefaultPipelineFuture.class);

    private static final long                                          DEFAULT_TIMEOUT_NANOSECONDS = TimeUnit.SECONDS
                                                                                                       .toNanos(30);

    /**
     * key: invokeId  value: future  该对象应该是封装了 pipeline 的所有future
     */
    private static final ConcurrentMap<Long, DefaultPipelineFuture<?>> futures                     = Maps
                                                                                                       .newConcurrentMapLong();

    /**
     * 代表当前future 的invokeId
     */
    private final long                                                 invokeId;
    private final long                                                 timeout;
    private final long                                                 startTime                   = System.nanoTime();

    /**
     * 创建一个新的future 对象
     * @param invokeId
     * @param timeoutMillis
     * @param <T>
     * @return
     */
    public static <T> DefaultPipelineFuture<T> with(final long invokeId, final long timeoutMillis) {
        return new DefaultPipelineFuture<>(invokeId, timeoutMillis);
    }

    public static void received(final long invokeId, final Object response) {
        final DefaultPipelineFuture<?> future = futures.remove(invokeId);
        if (future == null) {
            LOG.warn("A timeout response [{}] finally returned.", response);
            return;
        }
        future.doReceived(response);
    }

    private DefaultPipelineFuture(long invokeId, long timeoutMillis) {
        this.invokeId = invokeId;
        this.timeout = timeoutMillis > 0 ? TimeUnit.MILLISECONDS.toNanos(timeoutMillis) : DEFAULT_TIMEOUT_NANOSECONDS;
        futures.put(invokeId, this);
    }

    @Override
    public V getResult() throws Throwable {
        return get(timeout, TimeUnit.NANOSECONDS);
    }

    /**
     * 将 结果设置到 future中
     * @param response
     */
    @SuppressWarnings("unchecked")
    private void doReceived(final Object response) {
        if (response instanceof Throwable) {
            completeExceptionally((Throwable) response);
        } else {
            complete((V) response);
        }
    }

    // timeout scanner  该对象定时扫描 future中的过期任务 并设置过期异常以唤醒阻塞线程 类似于RocketMq 的 通信池
    @SuppressWarnings("all")
    private static class TimeoutScanner implements Runnable {

        public void run() {
            for (;;) {
                try {
                    for (final DefaultPipelineFuture<?> future : futures.values()) {
                        process(future);
                    }

                    Thread.sleep(30);
                } catch (Throwable t) {
                    LOG.error("An exception has been caught while scanning the timeout futures {}.", t);
                }
            }
        }

        private void process(final DefaultPipelineFuture<?> future) {
            if (future == null || future.isDone()) {
                return;
            }

            if (System.nanoTime() - future.startTime > future.timeout) {
                DefaultPipelineFuture.received(future.invokeId, new TimeoutException());
            }
        }
    }

    static {
        final Thread t = new Thread(new TimeoutScanner(), "timeout.scanner");
        t.setDaemon(true);
        t.start();
    }
}
