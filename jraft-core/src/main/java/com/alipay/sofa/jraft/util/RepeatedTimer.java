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
package com.alipay.sofa.jraft.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.timer.HashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.Timer;
import com.alipay.sofa.jraft.util.timer.TimerTask;

/**
 * Repeatable timer based on java.util.Timer.
 * 可重复定时器  基于 netty的 hash轮 实现
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-30 3:45:37 PM
 */
public abstract class RepeatedTimer implements Describer {

    public static final Logger LOG  = LoggerFactory.getLogger(RepeatedTimer.class);

    private final Lock         lock = new ReentrantLock();
    private final Timer        timer;
    /**
     * 该接口风格很像netty   包含任务结果相关信息
     */
    private Timeout            timeout;
    private boolean            stopped;
    // running 和 timeoutMs 都是敏感的
    private volatile boolean   running;
    private boolean            destroyed;
    private boolean            invoking;
    private volatile int       timeoutMs;
    private final String       name;

    public int getTimeoutMs() {
        return this.timeoutMs;
    }

    public RepeatedTimer(String name, int timeoutMs) {
        this(name, timeoutMs, new HashedWheelTimer(new NamedThreadFactory(name, true), 1, TimeUnit.MILLISECONDS, 2048));
    }

    public RepeatedTimer(String name, int timeoutMs, Timer timer) {
        super();
        this.name = name;
        this.timeoutMs = timeoutMs;
        this.stopped = true;
        this.timer = Requires.requireNonNull(timer, "timer");
    }

    /**
     * Subclasses should implement this method for timer trigger.
     * 代表任务逻辑
     */
    protected abstract void onTrigger();

    /**
     * Adjust timeoutMs before every scheduling.
     *
     * @param timeoutMs timeout millis
     * @return timeout millis
     */
    protected int adjustTimeout(final int timeoutMs) {
        return timeoutMs;
    }

    /**
     * 定时任务执行的逻辑
     */
    public void run() {
        this.lock.lock();
        try {
            this.invoking = true;
        } finally {
            this.lock.unlock();
        }
        try {
            onTrigger();
        } catch (final Throwable t) {
            LOG.error("Run timer failed.", t);
        }
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            // 代表业务逻辑已经执行完了
            this.invoking = false;
            if (this.stopped) {
                this.running = false;
                invokeDestroyed = this.destroyed;
            } else {
                // 开始重复下次任务
                this.timeout = null;
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
        // 如果定时器本身被关闭了
        if (invokeDestroyed) {
            onDestroy();
        }
    }

    /**
     * Run the timer at once, it will cancel the timer and re-schedule it.
     */
    public void runOnceNow() {
        this.lock.lock();
        try {
            // 关闭任务并执行
            if (this.timeout != null && this.timeout.cancel()) {
                this.timeout = null;
                run();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Called after destroy timer.
     */
    protected void onDestroy() {
        // NO-OP
    }

    /**
     * Start the timer.
     * 开启定时器
     */
    public void start() {
        this.lock.lock();
        try {
            // 代表已经销毁
            if (this.destroyed) {
                return;
            }
            // 代表已经启动
            if (!this.stopped) {
                return;
            }
            this.stopped = false;
            // 正在执行
            if (this.running) {
                return;
            }
            this.running = true;
            schedule();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 执行定时逻辑
     */
    private void schedule() {
        // 被 cancel 的任务不会执行了   应该是代表一次只能执行一个任务
        if(this.timeout != null) {
            this.timeout.cancel();
        }
        final TimerTask timerTask = timeout -> {
            try {
                RepeatedTimer.this.run();
            } catch (final Throwable t) {
                LOG.error("Run timer task failed, taskName={}.", RepeatedTimer.this.name, t);
            }
        };
        // adjustTimeout 是对用户开放的钩子 用于调整 时间间隔
        this.timeout = this.timer.newTimeout(timerTask, adjustTimeout(this.timeoutMs), TimeUnit.MILLISECONDS);
    }

    /**
     * Reset timer with new timeoutMs.
     *
     * @param timeoutMs timeout millis
     */
    public void reset(final int timeoutMs) {
        this.lock.lock();
        this.timeoutMs = timeoutMs;
        try {
            if (this.stopped) {
                return;
            }
            if (this.running) {
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Reset timer with current timeoutMs
     */
    public void reset() {
        this.lock.lock();
        try {
            reset(this.timeoutMs);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Destroy timer
     */
    public void destroy() {
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.destroyed = true;
            if (!this.running) {
                invokeDestroyed = true;
            }
            // Timer#stop is idempotent
            this.timer.stop();
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                if (this.timeout.cancel()) {
                    invokeDestroyed = true;
                    this.running = false;
                }
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
            if (invokeDestroyed) {
                onDestroy();
            }
        }
    }

    /**
     * Stop timer
     */
    public void stop() {
        this.lock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                this.timeout.cancel();
                this.running = false;
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final String _describeString;
        this.lock.lock();
        try {
            _describeString = toString();
        } finally {
            this.lock.unlock();
        }
        out.print("  ") //
            .println(_describeString);
    }

    @Override
    public String toString() {
        return "RepeatedTimer [timeout=" + this.timeout + ", stopped=" + this.stopped + ", running=" + this.running
               + ", destroyed=" + this.destroyed + ", invoking=" + this.invoking + ", timeoutMs=" + this.timeoutMs
               + "]";
    }
}
