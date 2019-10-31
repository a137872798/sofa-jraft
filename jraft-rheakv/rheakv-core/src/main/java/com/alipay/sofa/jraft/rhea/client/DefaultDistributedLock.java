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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.errors.InvalidLockAcquirerException;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

/**
 * Default implementation with distributed lock.
 * 默认的分布式锁实现   实际上实现通过 rheaKVStore
 * @author jiachun.fjc
 */
class DefaultDistributedLock extends DistributedLock<byte[]> {

    private static final Logger         LOG                = LoggerFactory.getLogger(DefaultDistributedLock.class);

    /**
     * 内部包含一个 KV 存储对象 底层基于 RocksDB
     */
    private final DefaultRheaKVStore    rheaKVStore;

    private volatile ScheduledFuture<?> watchdogFuture;
    private volatile boolean            mayCancelIfRunning = false;

    protected DefaultDistributedLock(byte[] target, long lease, TimeUnit unit, ScheduledExecutorService watchdog,
                                     DefaultRheaKVStore rheaKVStore) {
        super(target, lease, unit, watchdog);
        this.rheaKVStore = rheaKVStore;
    }

    /**
     * 解锁
     */
    @Override
    public void unlock() {
        // 获取锁对象的key
        final byte[] internalKey = getInternalKey();
        // 获取锁时使用的相关信息
        final Acquirer acquirer = getAcquirer();
        try {
            final Owner owner = this.rheaKVStore.releaseLockWith(internalKey, acquirer).get();
            // 等同于setOwner
            updateOwner(owner);
            // 此时发现 解锁后返回的锁持有者与 之前保存的不一致 抛出异常
            if (!owner.isSameAcquirer(acquirer)) {
                final String message = String.format(
                    "an invalid acquirer [%s] trying to unlock, the real owner is [%s]", acquirer, owner);
                throw new InvalidLockAcquirerException(message);
            }
            // 重试次数 小于0 就关闭本次任务
            if (owner.getAcquires() <= 0) {
                tryCancelScheduling();
            }
        } catch (final InvalidLockAcquirerException e) {
            LOG.error("Fail to unlock, {}.", StackTraceUtil.stackTrace(e));
            ThrowUtil.throwException(e);
        } catch (final Throwable t) {
            LOG.error("Fail to unlock: {}, will cancel scheduling, {}.", acquirer, StackTraceUtil.stackTrace(t));
            tryCancelScheduling();
            ThrowUtil.throwException(t);
        }
    }

    /**
     * 尝试针对 key 上锁
     * @param ctx
     * @return
     */
    @Override
    protected Owner internalTryLock(final byte[] ctx) {
        final byte[] internalKey = getInternalKey();
        final Acquirer acquirer = getAcquirer();
        acquirer.setContext(ctx);
        // 默认情况下不进行续约
        final CompletableFuture<Owner> future = this.rheaKVStore.tryLockWith(internalKey, false, acquirer);
        // 阻塞线程等待结果
        final Owner owner = FutureHelper.get(future);
        // 获取锁失败更新 owner
        if (!owner.isSuccess()) {
            updateOwner(owner);
            return owner;
        }

        // if success, update the fencing token in acquirer
        // 成功的话 更新owner 和 acq(内部的 fencingToken)
        updateOwnerAndAcquirer(owner);

        final ScheduledExecutorService watchdog = getWatchdog();
        if (watchdog == null) {
            return owner;
        }
        // schedule keeping lease   这里会自动为分布式锁进行续约  反过来理解 一开始锁的时间应该是短暂的 避免某台机器获取分布式锁后宕机导致锁无法释放
        if (this.watchdogFuture == null) {
            synchronized (this) {
                if (this.watchdogFuture == null) {
                    final long period = (acquirer.getLeaseMillis() / 3) << 1;
                    // 在2/3 的时间进行续约
                    this.watchdogFuture = scheduleKeepingLease(watchdog, internalKey, acquirer, period);
                }
            }
        }
        return owner;
    }

    /**
     * 定时对锁进行续约
     * @param watchdog
     * @param key
     * @param acquirer
     * @param period
     * @return
     */
    private ScheduledFuture<?> scheduleKeepingLease(final ScheduledExecutorService watchdog, final byte[] key,
                                                    final Acquirer acquirer, final long period) {
        return watchdog.scheduleAtFixedRate(() -> {
            try {
                // 每次执行定时任务前 判断该任务是否能够被关闭
                if (this.mayCancelIfRunning) {
                    // last time fail to cancel
                    tryCancelScheduling();
                    return;
                }
                // 以续约方式上锁 如果失败关闭本任务
                this.rheaKVStore.tryLockWith(key, true, acquirer).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Fail to keeping lease with lock: {}, {}.", acquirer, StackTraceUtil.stackTrace(throwable));
                        tryCancelScheduling();
                        return;
                    }
                    if (!result.isSuccess()) {
                        LOG.warn("Fail to keeping lease with lock: {}, and result detail is: {}.", acquirer, result);
                        tryCancelScheduling();
                        return;
                    }
                    LOG.debug("Keeping lease with lock: {}.", acquirer);
                });
            } catch (final Throwable t) {
                LOG.error("Fail to keeping lease with lock: {}, {}.", acquirer, StackTraceUtil.stackTrace(t));
                tryCancelScheduling();
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭定时器
     */
    private void tryCancelScheduling() {
        if (this.watchdogFuture != null) {
            this.mayCancelIfRunning = true;
            this.watchdogFuture.cancel(true);
        }
    }
}
