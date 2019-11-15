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
package com.alipay.sofa.jraft.core;

import java.util.concurrent.locks.StampedLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.Ballot;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.BallotBoxOptions;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Ballot box for voting.
 * @author boyan (boyan@alibaba-inc.com)
 * 投票箱
 * 2018-Apr-04 2:32:10 PM
 */
@ThreadSafe
public class BallotBox implements Lifecycle<BallotBoxOptions>, Describer {

    private static final Logger      LOG                = LoggerFactory.getLogger(BallotBox.class);

    /**
     * 内部存放了 状态机caller
     */
    private FSMCaller                waiter;
    /**
     * 回调队列  跟node中的 closureQueue 指向同一对象
     */
    private ClosureQueue             closureQueue;
    /**
     * 这个类没看过...  是一种优化的读写锁
     */
    private final StampedLock        stampedLock        = new StampedLock();
    /**
     * 最后提交的下标  默认为0 需要配合 setLastCommittedIndex
     */
    private long                     lastCommittedIndex = 0;
    /**
     * 代表在整个投票箱中 当前最前的一个悬置任务下标  初始状态下等同于 lastLogIndex
     */
    private long                     pendingIndex;
    /**
     * 存放待投票的任务
     */
    private final ArrayDeque<Ballot> pendingMetaQueue   = new ArrayDeque<>();

    @OnlyForTest
    long getPendingIndex() {
        return this.pendingIndex;
    }

    @OnlyForTest
    ArrayDeque<Ballot> getPendingMetaQueue() {
        return this.pendingMetaQueue;
    }

    /**
     * 获取 lastCommittedIndex  下面的方法 抛开 stampedLock的api 不看 实际上就只是获取了lastCommittedIndex
     * @return
     */
    public long getLastCommittedIndex() {
        // 进行乐观读
        long stamp = this.stampedLock.tryOptimisticRead();
        final long optimisticVal = this.lastCommittedIndex;
        if (this.stampedLock.validate(stamp)) {
            return optimisticVal;
        }
        stamp = this.stampedLock.readLock();
        try {
            return this.lastCommittedIndex;
        } finally {
            this.stampedLock.unlockRead(stamp);
        }
    }

    /**
     * 当投票箱对象被初始化时
     * @param opts
     * @return
     */
    @Override
    public boolean init(final BallotBoxOptions opts) {
        // 必须包含caller 和回调队列
        if (opts.getWaiter() == null || opts.getClosureQueue() == null) {
            LOG.error("waiter or closure queue is null.");
            return false;
        }
        this.waiter = opts.getWaiter();
        this.closureQueue = opts.getClosureQueue();
        return true;
    }

    /**
     * Called by leader, otherwise the behavior is undefined
     * Set logs in [first_log_index, last_log_index] are stable at |peer|.
     * 每当某个节点成功将数据刷盘时就会触发该方法
     * @param peer 本次提交成功的节点信息 包含 endpoint
     * @param firstLogIndex 对应该entry 写入到logManager 的起点
     * @param lastLogIndex 任务总数 + firstLogIndex
     */
    public boolean commitAt(final long firstLogIndex, final long lastLogIndex, final PeerId peer) {
        // TODO  use lock-free algorithm here?
        final long stamp = stampedLock.writeLock();
        long lastCommittedIndex = 0;
        try {
            // 当某个节点变成leader 后该值就会设置成非0的值
            if (this.pendingIndex == 0) {
                return false;
            }

            // lastLogIndex 是本次投票对应的数据的lastIndex 而pendingIndex 对应当前维护的所有悬置任务中 已经超过半数
            // 刷盘成功的起点下标 这里代表已经处理过
            if (lastLogIndex < this.pendingIndex) {
                return true;
            }

            // 当前提交的长度不能超过 等待队列的总长度 lastLogIndex 和 pendingMetaQueue.size() 中每个单位长度都是entry
            // 它们是对等的
            if (lastLogIndex >= this.pendingIndex + this.pendingMetaQueue.size()) {
                throw new ArrayIndexOutOfBoundsException();
            }

            // 很大程度上 appendPend和 从复制机发送数据的顺序是一致的  通过复制机的优先队列 接收顺序与发送顺序一致 那么
            // appendPend 和commitAt的顺序基本一致  appendPend 和setLastLogInde 的顺序又是一致的(同一条线程顺序执行)
            // 唯一可能出现乱序的情况 就是往线程池提交任务时某条线程执行的慢而后面的线程执行的快
            // 那么 startAt 基本可以看作是 pendingIndex 这样下面就不会出现无法预测的情况
            final long startAt = Math.max(this.pendingIndex, firstLogIndex);
            // 这里初始化了一个 轨迹对象
            Ballot.PosHint hint = new Ballot.PosHint();
            // 每次触发的可能是一组entry 所以这里是一个循环
            for (long logIndex = startAt; logIndex <= lastLogIndex; logIndex++) {

                final Ballot bl = this.pendingMetaQueue.get((int) (logIndex - this.pendingIndex));
                // 代表针对该投票对象 peer 已经提交成功
                hint = bl.grant(peer, hint);
                // 如果本次作为能满足超过半数确认票的情况
                if (bl.isGranted()) {
                    lastCommittedIndex = logIndex;
                }
            }
            // 代表还没有超过半数 不会触发状态机的 onCommited
            if (lastCommittedIndex == 0) {
                return true;
            }
            // When removing a peer off the raft group which contains even number of
            // peers, the quorum would decrease by 1, e.g. 3 of 4 changes to 2 of 3. In
            // this case, the log after removal may be committed before some previous
            // logs, since we use the new configuration to deal the quorum of the
            // removal request, we think it's safe to commit all the uncommitted
            // previous logs, which is not well proved right now
            // 这里就按照这样实现了 实际上前面的数据 可能还没有成功写入半数
            this.pendingMetaQueue.removeRange(0, (int) (lastCommittedIndex - this.pendingIndex) + 1);
            LOG.debug("Committed log fromIndex={}, toIndex={}.", this.pendingIndex, lastCommittedIndex);
            this.pendingIndex = lastCommittedIndex + 1;
            this.lastCommittedIndex = lastCommittedIndex;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
        // 触发 状态机的commited 只有首次超过半数时触发
        // 相当于是通过stateMachine 转发给node 提交任务之后通过ballotBox 在确认票数超过半数时 回调node的caller 对象
        this.waiter.onCommitted(lastCommittedIndex);
        return true;
    }

    /**
     * Called when the leader steps down, otherwise the behavior is undefined
     * When a leader steps down, the uncommitted user applications should
     * fail immediately, which the new leader will deal whether to commit or
     * truncate.
     * 在 写锁加持下 清空队列
     */
    public void clearPendingTasks() {
        final long stamp = this.stampedLock.writeLock();
        try {
            this.pendingMetaQueue.clear();
            this.pendingIndex = 0;
            this.closureQueue.clear();
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called when a candidate becomes the new leader, otherwise the behavior is
     * undefined.
     * According the the raft algorithm, the logs from previous terms can't be
     * committed until a log at the new term becomes committed, so
     * |newPendingIndex| should be |last_log_index| + 1.
     *
     * 当某个对象变成leader后触发
     * @param newPendingIndex pending index of new leader
     * @return returns true if reset success
     */
    public boolean resetPendingIndex(final long newPendingIndex) {
        final long stamp = this.stampedLock.writeLock();
        try {
            // 调用该方法前 必须确保 属性为初始状态  那么重置就是在 stepDown(变成follower)时执行
            if (!(this.pendingIndex == 0 && this.pendingMetaQueue.isEmpty())) {
                LOG.error("resetPendingIndex fail, pendingIndex={}, pendingMetaQueueSize={}.", this.pendingIndex,
                    this.pendingMetaQueue.size());
                return false;
            }
            // 新的悬置index起点  必须大于最后提交的偏移量, 它是下次提交数据的起点
            if (newPendingIndex <= this.lastCommittedIndex) {
                LOG.error("resetPendingIndex fail, newPendingIndex={}, lastCommittedIndex={}.", newPendingIndex,
                    this.lastCommittedIndex);
                return false;
            }
            // 该值代表已经提交到了哪里  (也就是在半数node刷盘成功)
            this.pendingIndex = newPendingIndex;
            // 这里设置下标是做什么???
            this.closureQueue.resetFirstIndex(newPendingIndex);
            return true;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called by leader, otherwise the behavior is undefined
     * Store application context before replication.
     *
     * 向投票箱设置等待中的任务 (也就是需要等待成功刷盘到半数follower)
     * @param conf      current configuration   一个集群
     * @param oldConf   old configuration       旧集群节点
     * @param done      callback                处理完成后的回调方法
     * @return          returns true on success 判断添加是否成功
     */
    public boolean appendPendingTask(final Configuration conf, final Configuration oldConf, final Closure done) {
        final Ballot bl = new Ballot();
        // ballot 对象内部 包含需要访问的所有节点
        if (!bl.init(conf, oldConf)) {
            LOG.error("Fail to init ballot.");
            return false;
        }
        // 改进的写锁
        final long stamp = this.stampedLock.writeLock();
        try {
            // 确保pendingIndex 大于 0 默认值应该就是0
            // 难道是在定时任务中做了什么 但是这样就成异步了啊 也就是用户一旦提交任务 无需确定是否投票了 完全根据回调对象的结果来确定执行逻辑
            if (this.pendingIndex <= 0) {
                LOG.error("Fail to appendingTask, pendingIndex={}.", this.pendingIndex);
                return false;
            }
            // 添加到 回调队列和 悬置队列中
            this.pendingMetaQueue.add(bl);
            this.closureQueue.appendPendingClosure(done);
            return true;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called by follower, otherwise the behavior is undefined.
     *  Set committed index received from leader
     * 该方法是在初始化投票箱对象 只有pendingIndex 被设置后 才能正常提交任务 以及触发 commitAt
     * 该方法的调用时机先不管  总之是在向状态机提交任务之前
     * @param lastCommittedIndex last committed index
     * @return returns true if set success
     */
    public boolean setLastCommittedIndex(final long lastCommittedIndex) {
        boolean doUnlock = true;
        final long stamp = this.stampedLock.writeLock();
        try {
            // 悬置相关属性必须为空
            if (this.pendingIndex != 0 || !this.pendingMetaQueue.isEmpty()) {
                Requires.requireTrue(lastCommittedIndex < this.pendingIndex,
                    "Node changes to leader, pendingIndex=%d, param lastCommittedIndex=%d", this.pendingIndex,
                    lastCommittedIndex);
                return false;
            }
            if (lastCommittedIndex < this.lastCommittedIndex) {
                return false;
            }
            if (lastCommittedIndex > this.lastCommittedIndex) {
                this.lastCommittedIndex = lastCommittedIndex;
                this.stampedLock.unlockWrite(stamp);
                doUnlock = false;
                // 将耗时操作 挪到外面执行  这里为什么会触发 caller 的 COMMITED???
                this.waiter.onCommitted(lastCommittedIndex);
            }
        } finally {
            if (doUnlock) {
                this.stampedLock.unlockWrite(stamp);
            }
        }
        return true;
    }

    /**
     * 对应 LifeCycle 的终止方法 清空所有悬置任务
     */
    @Override
    public void shutdown() {
        clearPendingTasks();
    }

    @Override
    public void describe(final Printer out) {
        long _lastCommittedIndex;
        long _pendingIndex;
        long _pendingMetaQueueSize;
        long stamp = this.stampedLock.tryOptimisticRead();
        if (this.stampedLock.validate(stamp)) {
            _lastCommittedIndex = this.lastCommittedIndex;
            _pendingIndex = this.pendingIndex;
            _pendingMetaQueueSize = this.pendingMetaQueue.size();
        } else {
            stamp = this.stampedLock.readLock();
            try {
                _lastCommittedIndex = this.lastCommittedIndex;
                _pendingIndex = this.pendingIndex;
                _pendingMetaQueueSize = this.pendingMetaQueue.size();
            } finally {
                this.stampedLock.unlockRead(stamp);
            }
        }
        out.print("  lastCommittedIndex: ") //
            .println(_lastCommittedIndex);
        out.print("  pendingIndex: ") //
            .println(_pendingIndex);
        out.print("  pendingMetaQueueSize: ") //
            .println(_pendingMetaQueueSize);
    }
}
