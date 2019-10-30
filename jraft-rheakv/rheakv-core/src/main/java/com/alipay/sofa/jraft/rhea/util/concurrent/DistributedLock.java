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
package com.alipay.sofa.jraft.rhea.util.concurrent;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.rhea.util.UniqueIdUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;

/**
 * A distributed lock that provides exclusive access to a
 * shared resource.
 *
 * <pre>
 *      DistributedLock<T> lock = ...;
 *      if (lock.tryLock()) {
 *          try {
 *              // manipulate protected state
 *          } finally {
 *              lock.unlock();
 *          }
 *      } else {
 *          // perform alternative actions
 *      }
 * </pre>
 * 分布式锁 骨架类
 * @author jiachun.fjc
 */
public abstract class DistributedLock<T> {

    /**
     * 锁对象 由用户定义
     */
    private final T                        internalKey;
    /**
     * 用于获取锁的对象
     */
    private final Acquirer                 acquirer;
    /**
     * 监视器
     */
    private final ScheduledExecutorService watchdog;

    /**
     * 用于描述当前获得锁的对象  实际上也是通过 在KVStore 中 设置一个 分布式环境的唯一对象 并标识该对象是由谁来持有
     */
    private volatile Owner                 owner;

    protected DistributedLock(T target, long lease, TimeUnit unit, ScheduledExecutorService watchdog) {
        Requires.requireTrue(lease >= 0, "lease must >= 0");
        this.internalKey = withInternalKey(target);
        this.acquirer = new Acquirer(UniqueIdUtil.generateId(), unit.toMillis(lease));
        this.watchdog = watchdog;
    }

    /**
     * @see #tryLock(byte[])
     */
    public boolean tryLock() {
        return tryLock(null);
    }

    /**
     * Acquires the lock only if it is free at the time of invocation.
     *
     * Acquires the lock if it is available and returns immediately
     * with the value {@code true}.
     * If the lock is not available then this method will return
     * immediately with the value {@code false}.
     *
     * @param ctx the context of current lock request
     * @return {@code true} if the lock was acquired and {@code false}
     * otherwise
     */
    public boolean tryLock(final byte[] ctx) {
        return internalTryLock(ctx).isSuccess();
    }

    /**
     * @see #tryLock(byte[], long, TimeUnit)
     */
    public boolean tryLock(final long timeout, final TimeUnit unit) {
        return tryLock(null, timeout, unit);
    }

    /**
     * Acquires the lock if it is free within the given waiting time.
     *
     * If the lock is available this method returns immediately
     * with the value {@code true}.
     * If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens:
     * <ul>
     * <li>The lock is acquired by the current thread; or
     * <li>The specified waiting time elapses
     *
     * @param ctx     the context of current lock request
     * @param timeout the maximum time to wait for the lock
     * @param unit    the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired and {@code false}
     * if the waiting time elapsed before the lock was acquired
     * 以 ctx 作为 锁的key 尝试加锁 timeout 代表最大等待锁时间
     */
    public boolean tryLock(final byte[] ctx, final long timeout, final TimeUnit unit) {
        final long timeoutNs = unit.toNanos(timeout);
        final long startNs = System.nanoTime();
        int attempts = 1;
        try {
            for (;;) {
                final Owner owner = internalTryLock(ctx);
                // 代表获取锁成功
                if (owner.isSuccess()) {
                    return true;
                }
                // 超过尝试获取锁的最大时间
                if (System.nanoTime() - startNs >= timeoutNs) {
                    break;
                }
                // 该值 相当于一个睡眠的时间
                if (attempts < 8) {
                    attempts++;
                }
                final long remaining = Math.max(0, owner.getRemainingMillis());
                // TODO optimize with notify?
                // avoid liveLock  每次睡眠的时间都会延长 有点类似网络重连的间隔延长 这里是在避免活锁
                Thread.sleep(Math.min(remaining, 2 << attempts));
            }
        } catch (final Throwable t) {
            ThrowUtil.throwException(t);
        }
        return false;
    }

    /**
     * Attempts to release this lock.
     *
     * If the current caller is the holder of this lock then the hold
     * count is decremented.  If the hold count is now zero then the
     * lock is released.  If the current caller is not the holder of
     * this lock then InvalidLockAcquirerException is thrown.
     */
    public abstract void unlock();

    /**
     * Making the lock safe with fencing token.
     *
     * Is simply a number that increases (e.g. incremented by the lock
     * service) every time a client acquires the lock.
     */
    public long getFencingToken() {
        return this.acquirer.getFencingToken();
    }

    /**
     * Returns the 'lock-context' of current lock owner.
     */
    public byte[] getOwnerContext() {
        return getOwner().getContext();
    }

    public ScheduledExecutorService getWatchdog() {
        return watchdog;
    }

    public Acquirer getAcquirer() {
        return acquirer;
    }

    public Owner getOwner() {
        final Owner copy = this.owner;
        if (copy == null) {
            throw new IllegalStateException("must try to lock at first");
        }
        return copy;
    }

    public static OwnerBuilder newOwnerBuilder() {
        return new OwnerBuilder();
    }

    /**
     * 尝试获取 对应key 的 锁对象 owner 代表本对象获取锁的信息 内部包含一个 success 属性 用于判断本次申请锁是否成功
     * @param ctx
     * @return
     */
    protected abstract Owner internalTryLock(final byte[] ctx);

    protected T withInternalKey(final T target) {
        // override this method to plastic with target, default do nothing
        return target;
    }

    protected T getInternalKey() {
        return internalKey;
    }

    protected void updateOwner(final Owner owner) {
        this.owner = owner;
    }

    protected void updateOwnerAndAcquirer(final Owner owner) {
        this.owner = owner;
        if (this.owner != null) {
            this.owner.updateAcquirerInfo(this.acquirer);
        }
    }

    public static class Acquirer implements Serializable {

        private static final long serialVersionUID = -9174459539789423607L;

        private final String      id;
        private final long        leaseMillis;

        // the time on trying to lock, it must be set by lock server  尝试获取锁的时间戳 必须由server 来设置 可能有什么特殊含义
        private volatile long     lockingTimestamp;
        // making the lock safe with fencing token.
        //
        // is simply a number that increases (e.g. incremented by the lock service)
        // every time a client acquires the lock.
        private volatile long     fencingToken;
        // the context of current lock request
        private volatile byte[]   context;

        public Acquirer(String id, long leaseMillis) {
            this.id = id;
            this.leaseMillis = leaseMillis;
        }

        public String getId() {
            return id;
        }

        public long getLeaseMillis() {
            return leaseMillis;
        }

        public long getLockingTimestamp() {
            return lockingTimestamp;
        }

        public void setLockingTimestamp(long lockingTimestamp) {
            this.lockingTimestamp = lockingTimestamp;
        }

        public long getFencingToken() {
            return fencingToken;
        }

        public void setFencingToken(long fencingToken) {
            this.fencingToken = fencingToken;
        }

        public byte[] getContext() {
            return context;
        }

        public void setContext(byte[] context) {
            this.context = context;
        }

        @Override
        public String toString() {
            return "Acquirer{" + "id='" + id + '\'' + ", leaseMillis=" + leaseMillis + ", lockingTimestamp="
                   + lockingTimestamp + ", fencingToken=" + fencingToken + ", context=" + BytesUtil.toHex(context)
                   + '}';
        }
    }

    /**
     * 锁的持有者
     */
    public static class Owner implements Serializable {

        private static final long serialVersionUID = 3939239434225894164L;

        // locker id  锁对象的 标识
        private final String      id;
        // absolute time for this lock to expire  代表还有多久锁会过期
        private final long        deadlineMillis;
        // remainingMillis < 0 means lock successful   代表 还有多久能获取到锁
        private final long        remainingMillis;
        // making the lock safe with fencing token
        //
        // is simply a number that increases (e.g. incremented by the lock service)
        // every time a client acquires the lock.   通过一个自增序列 来确保锁安全???
        private final long        fencingToken;
        // for reentrant lock  当前重入次数
        private final long        acquires;
        // the context of current lock owner  描述当前锁的持有者信息
        private final byte[]      context;
        // if operation success   本次获取锁的操作是否成功
        private final boolean     success;

        public Owner(String id, long deadlineMillis, long remainingMillis, long fencingToken, long acquires,
                     byte[] context, boolean success) {
            this.id = id;
            this.deadlineMillis = deadlineMillis;
            this.remainingMillis = remainingMillis;
            this.fencingToken = fencingToken;
            this.acquires = acquires;
            this.context = context;
            this.success = success;
        }

        /**
         * 判断 尝试获取锁的2个对象是否一致  这里只要 token 和 id 一致就可以
         * @param acquirer
         * @return
         */
        public boolean isSameAcquirer(final Acquirer acquirer) {
            return acquirer != null && this.fencingToken == acquirer.fencingToken
                   && Objects.equals(this.id, acquirer.id);
        }

        public void updateAcquirerInfo(final Acquirer acquirer) {
            if (acquirer == null) {
                return;
            }
            acquirer.setFencingToken(this.fencingToken);
        }

        public String getId() {
            return id;
        }

        public long getDeadlineMillis() {
            return deadlineMillis;
        }

        public long getRemainingMillis() {
            return remainingMillis;
        }

        public long getFencingToken() {
            return fencingToken;
        }

        public long getAcquires() {
            return acquires;
        }

        public byte[] getContext() {
            return context;
        }

        public boolean isSuccess() {
            return success;
        }

        @Override
        public String toString() {
            return "Owner{" + "id='" + id + '\'' + ", deadlineMillis=" + deadlineMillis + ", remainingMillis="
                   + remainingMillis + ", fencingToken=" + fencingToken + ", acquires=" + acquires + ", context="
                   + BytesUtil.toHex(context) + ", success=" + success + '}';
        }
    }

    public static class OwnerBuilder {

        public static long KEEP_LEASE_FAIL     = Long.MAX_VALUE;
        public static long FIRST_TIME_SUCCESS  = -1;
        public static long NEW_ACQUIRE_SUCCESS = -2;
        public static long KEEP_LEASE_SUCCESS  = -3;
        public static long REENTRANT_SUCCESS   = -4;

        private String     id;
        private long       deadlineMillis;
        private long       remainingMillis;
        private long       fencingToken;
        private long       acquires;
        private byte[]     context;
        private boolean    success;

        public Owner build() {
            return new Owner(this.id, this.deadlineMillis, this.remainingMillis, this.fencingToken, this.acquires,
                this.context, this.success);
        }

        public OwnerBuilder id(final String id) {
            this.id = id;
            return this;
        }

        public OwnerBuilder deadlineMillis(final long deadlineMillis) {
            this.deadlineMillis = deadlineMillis;
            return this;
        }

        public OwnerBuilder remainingMillis(final long remainingMillis) {
            this.remainingMillis = remainingMillis;
            return this;
        }

        public OwnerBuilder fencingToken(final long fencingToken) {
            this.fencingToken = fencingToken;
            return this;
        }

        public OwnerBuilder acquires(final long acquires) {
            this.acquires = acquires;
            return this;
        }

        public OwnerBuilder context(final byte[] context) {
            this.context = context;
            return this;
        }

        public OwnerBuilder success(final boolean success) {
            this.success = success;
            return this;
        }
    }
}
