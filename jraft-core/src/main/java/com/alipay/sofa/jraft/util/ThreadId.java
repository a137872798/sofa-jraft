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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replicator id with lock.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-29 10:59:47 AM
 */
public class ThreadId {

    private static final Logger    LOG                 = LoggerFactory.getLogger(ThreadId.class);

    /**
     * 尝试上锁超时时间
     */
    private static final int       TRY_LOCK_TIMEOUT_MS = 10;

    /**
     * 数据体
     */
    private final Object           data;
    /**
     * 非重入锁  什么时候会用到该对象???
     */
    private final NonReentrantLock lock                = new NonReentrantLock();
    /**
     * 闲置的异常对象
     */
    private final List<Integer>    pendingErrors       = Collections.synchronizedList(new ArrayList<>());
    /**
     * 异常回调对象
     */
    private final OnError          onError;
    /**
     * 是否已经被销毁
     */
    private volatile boolean       destroyed;

    /**
     * 一个异常回调对象
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Mar-29 11:01:54 AM
     */
    public interface OnError {

        /**
         * Error callback, it will be called in lock, but should take care of unlocking it.
         *
         * @param id        the thread id
         * @param data      the data
         * @param errorCode the error code
         */
        void onError(final ThreadId id, final Object data, final int errorCode);
    }

    public ThreadId(final Object data, final OnError onError) {
        super();
        this.data = data;
        this.onError = onError;
        this.destroyed = false;
    }

    public Object getData() {
        return this.data;
    }

    /**
     * 对该对象进行上锁
     * @return
     */
    public Object lock() {
        // 已经销毁的情况不做处理
        if (this.destroyed) {
            return null;
        }
        try {
            while (!this.lock.tryLock(TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                if (this.destroyed) {
                    return null;
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); // reset
            return null;
        }
        // Got the lock, double checking state.
        if (this.destroyed) {
            // should release lock
            this.lock.unlock();
            return null;
        }
        // 上锁后 返回 data
        return this.data;
    }

    /**
     * 进行解锁
     */
    public void unlock() {
        if (!this.lock.isHeldByCurrentThread()) {
            LOG.warn("Fail to unlock with {}, the lock is held by {} and current thread is {}.", this.data,
                this.lock.getOwner(), Thread.currentThread());
            return;
        }
        // calls all pending errors before unlock
        boolean doUnlock = true;
        try {
            // 解锁时 将所有 异常 取出来 并使用回调对象处理
            final List<Integer> errors;
            synchronized (this.pendingErrors) {
                errors = new ArrayList<>(this.pendingErrors);
                this.pendingErrors.clear();
            }
            for (final Integer code : errors) {
                // The lock will be unlocked in onError.
                doUnlock = false;
                if (this.onError != null) {
                    this.onError.onError(this, this.data, code);
                }
            }
        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
    }

    public void join() {
        while (!this.destroyed) {
            ThreadHelper.onSpinWait();
        }
    }

    @Override
    public String toString() {
        return this.data.toString();
    }

    /**
     * 销毁后就不允许再加锁了
     */
    public void unlockAndDestroy() {
        if (this.destroyed) {
            return;
        }
        this.destroyed = true;
        if (!this.lock.isHeldByCurrentThread()) {
            LOG.warn("Fail to unlockAndDestroy with {}, the lock is held by {} and current thread is {}.", this.data,
                this.lock.getOwner(), Thread.currentThread());
            return;
        }
        this.lock.unlock();
    }

    /**
     * Set error code, if it tryLock success, run the onError callback
     * with code immediately, else add it into pending errors and will
     * be called before unlock.
     *
     * @param errorCode error code
     */
    public void setError(final int errorCode) {
        if (this.destroyed) {
            return;
        }
        if (this.lock.tryLock()) {
            if (this.destroyed) {
                this.lock.unlock();
                return;
            }
            if (this.onError != null) {
                // The lock will be unlocked in onError.
                this.onError.onError(this, this.data, errorCode);
            }
        } else {
            // 如果上锁失败就将 异常保存到队列中
            this.pendingErrors.add(errorCode);
        }
    }
}
