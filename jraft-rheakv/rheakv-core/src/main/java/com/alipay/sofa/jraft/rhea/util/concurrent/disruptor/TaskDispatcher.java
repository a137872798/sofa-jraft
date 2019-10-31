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
package com.alipay.sofa.jraft.rhea.util.concurrent.disruptor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.util.Ints;
import com.alipay.sofa.jraft.util.Requires;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * The default wait strategy used by the Disruptor is the BlockingWaitStrategy.
 *
 * BlockingWaitStrategy:
 * Internally the BlockingWaitStrategy uses a typical lock and condition variable to handle thread wake-up.
 * The BlockingWaitStrategy is the slowest of the available wait strategies,
 * but is the most conservative with the respect to CPU usage and will give the most consistent behaviour across
 * the widest variety of deployment options. However, again knowledge of the deployed system can allow for additional
 * performance.
 *
 * SleepingWaitStrategy:
 * Like the BlockingWaitStrategy the SleepingWaitStrategy it attempts to be conservative with CPU usage,
 * by using a simple busy wait loop, but uses a call to LockSupport.parkNanos(1) in the middle of the loop.
 * On a typical Linux system this will pause the thread for around 60µs.
 * However it has the benefit that the producing thread does not need to take any action other increment the appropriate
 * counter and does not require the cost of signalling a condition variable. However, the mean latency of moving the
 * event between the producer and consumer threads will be higher. It works best in situations where low latency is not
 * required, but a low impact on the producing thread is desired. A common use case is for asynchronous logging.
 *
 * YieldingWaitStrategy:
 * The YieldingWaitStrategy is one of 2 Wait Strategies that can be use in low latency systems,
 * where there is the option to burn CPU cycles with the goal of improving latency.
 * The YieldingWaitStrategy will busy spin waiting for the sequence to increment to the appropriate value.
 * Inside the body of the loop Thread.yield() will be called allowing other queued threads to run.
 * This is the recommended wait strategy when need very high performance and the number of Event Handler threads is
 * less than the total number of logical cores, e.g. you have hyper-threading enabled.
 *
 * BusySpinWaitStrategy:
 * The BusySpinWaitStrategy is the highest performing Wait Strategy, but puts the highest constraints on the deployment
 * environment. This wait strategy should only be used if the number of Event Handler threads is smaller than the number
 * of physical cores on the box. E.g. hyper-threading should be disabled
 *
 * 任务分发器
 * @author jiachun.fjc
 */
public class TaskDispatcher implements Dispatcher<Runnable> {

    /**
     * 生产者
     */
    private static final EventFactory<MessageEvent<Runnable>> eventFactory = MessageEvent::new;

    /**
     * 高性能并发队列
     */
    private final Disruptor<MessageEvent<Runnable>> disruptor;

    /**
     * 初始化任务分发器
     * @param bufSize
     * @param numWorkers
     * @param waitStrategyType
     * @param threadFactory
     */
    public TaskDispatcher(int bufSize, int numWorkers, WaitStrategyType waitStrategyType, ThreadFactory threadFactory) {
        Requires.requireTrue(bufSize > 0, "bufSize must be larger than 0");
        if (!Ints.isPowerOfTwo(bufSize)) {
            bufSize = Ints.roundToPowerOfTwo(bufSize);
        }
        WaitStrategy waitStrategy;
        // 这里对应消费者无数据消费时采用的阻塞策略
        switch (waitStrategyType) {
            case BLOCKING_WAIT:
                waitStrategy = new BlockingWaitStrategy();
                break;
            case LITE_BLOCKING_WAIT:
                waitStrategy = new LiteBlockingWaitStrategy();
                break;
            case TIMEOUT_BLOCKING_WAIT:
                waitStrategy = new TimeoutBlockingWaitStrategy(1000, TimeUnit.MILLISECONDS);
                break;
            case LITE_TIMEOUT_BLOCKING_WAIT:
                waitStrategy = new LiteTimeoutBlockingWaitStrategy(1000, TimeUnit.MILLISECONDS);
                break;
            case PHASED_BACK_OFF_WAIT:
                waitStrategy = PhasedBackoffWaitStrategy.withLiteLock(1000, 1000, TimeUnit.NANOSECONDS);
                break;
            case SLEEPING_WAIT:
                waitStrategy = new SleepingWaitStrategy();
                break;
            case YIELDING_WAIT:
                waitStrategy = new YieldingWaitStrategy();
                break;
            case BUSY_SPIN_WAIT:
                waitStrategy = new BusySpinWaitStrategy();
                break;
            default:
                throw new UnsupportedOperationException(waitStrategyType.toString());
        }
        // 代表以多生产者方式初始化 并发队列
        this.disruptor = new Disruptor<>(eventFactory, bufSize, threadFactory, ProducerType.MULTI, waitStrategy);
        this.disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());
        if (numWorkers == 1) {
            // 使用单个任务处理器  实际上就是调用message.run()
            this.disruptor.handleEventsWith(new TaskHandler());
        } else {
            // 生成多个消费者
            final TaskHandler[] handlers = new TaskHandler[numWorkers];
            for (int i = 0; i < numWorkers; i++) {
                handlers[i] = new TaskHandler();
            }
            // 以一个工作者池分摊任务  这里多个消费者 CAS 修改偏移量 成功的那个就按照那个偏移量消费任务
            this.disruptor.handleEventsWithWorkerPool(handlers);
        }
        this.disruptor.start();
    }

    /**
     * 处理任务的逻辑
     * @param message
     * @return
     */
    @Override
    public boolean dispatch(final Runnable message) {
        final RingBuffer<MessageEvent<Runnable>> ringBuffer = disruptor.getRingBuffer();
        try {
            // 获取生产者的偏移量
            final long sequence = ringBuffer.tryNext();
            try {
                final MessageEvent<Runnable> event = ringBuffer.get(sequence);
                // 设置事件
                event.setMessage(message);
            } finally {
                // 发布事件唤醒消费者
                ringBuffer.publish(sequence);
            }
            return true;
        } catch (final InsufficientCapacityException e) {
            // This exception is used by the Disruptor as a global goto. It is a singleton
            // and has no stack trace.  Don't worry about performance.
            return false;
        }
    }

    /**
     * 执行某条消息 优先发布到 disruptor 失败的情况下 直接处理message
     * @param message
     */
    @Override
    public void execute(final Runnable message) {
        if (!dispatch(message)) {
            message.run(); // If fail to dispatch, caller run.
        }
    }

    @Override
    public void shutdown() {
        this.disruptor.shutdown();
    }
}
