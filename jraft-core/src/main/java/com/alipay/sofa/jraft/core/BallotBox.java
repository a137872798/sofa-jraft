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
     * 内部存放了 状态机对象
     */
    private FSMCaller                waiter;
    /**
     * 回调队列
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
     * 闲置下标
     */
    private long                     pendingIndex;
    /**
     * 悬置队列  多批node 任务会存放在该队列中 通过 pendingIndex 作为分界线
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
        // 必须包含状态机 和回调队列
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
     * 代表投票箱 往某个节点插入数据成功  每个节点内部包含一个 LogManager 该对象用于写入数据 一旦写入代表持久化成功 当半数节点提交成功时
     * 代表本次任务提交成功
     * 同时用户一次提交多少任务 在这里就会收到多少数据
     * @param peer 本次提交成功的节点信息 包含 endpoint
     * @param firstLogIndex 对应该entry 写入到logManager 的起点
     * @param lastLogIndex 任务总数 + firstLogIndex
     */
    public boolean commitAt(final long firstLogIndex, final long lastLogIndex, final PeerId peer) {
        // TODO  use lock-free algorithm here?  这行注解是什么意思 应该还是要加锁吧
        final long stamp = stampedLock.writeLock();
        long lastCommittedIndex = 0;
        try {
            if (this.pendingIndex == 0) {
                return false;
            }
            // 推测 pendingIndex 对应某次任务的分界线 因为该对象会暂存node 每次保存的entry 多批的话如果用一个容器存需要一个分界线
            // 而只有确保了 任务超过半数成功 pendingIndex 才会修改  通过一旦超过半数之后的修改ballot就不需要执行了
            if (lastLogIndex < this.pendingIndex) {
                return true;
            }

            // 当前提交的长度不能超过 等待队列的总长度
            if (lastLogIndex >= this.pendingIndex + this.pendingMetaQueue.size()) {
                throw new ArrayIndexOutOfBoundsException();
            }

            // 这里在确定起点 选择大的那个  思考一下 如果发生了乱序 firstLogIndex > pendingIndex 也就是后面的数据先触发了回调
            // TODO 乱序的前提是本节点是follower 因为leader 是同一进程写入不会发生乱序
            final long startAt = Math.max(this.pendingIndex, firstLogIndex);
            // 这里初始化了一个 轨迹对象
            Ballot.PosHint hint = new Ballot.PosHint();
            for (long logIndex = startAt; logIndex <= lastLogIndex; logIndex++) {
                // 获取对应的 投票对象   因为 node 可能一次会提交多个任务 每个任务 都要投票成功
                // TODO 等下 用户提交一批任务时 如果某几个失败会怎样??? 剩下的会当作成功的处理吗 然后失败的用户重新投递???
                // TODO 不过这种情况只会发生在 没有在unfoundNode中找到匹配节点 该种情况可能发生吗
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
            // 这里丢弃的数量虽然是正常的 但是如果后面写入的数据 先触发回调了 那么这里删除的却是前面的任务 还有
            // 正确的使用方式是否是 必须在某个数据提交并确认后才允许提交下批任务呢
            this.pendingMetaQueue.removeRange(0, (int) (lastCommittedIndex - this.pendingIndex) + 1);
            LOG.debug("Committed log fromIndex={}, toIndex={}.", this.pendingIndex, lastCommittedIndex);
            // 更新与下次提交数据的分界线   如果 乱序的话 pendingIndex 会在后面
            // 会使得             if (lastLogIndex < this.pendingIndex) {  这样判断过不了 也就无法对前面的数据进行grant了
            this.pendingIndex = lastCommittedIndex + 1;
            this.lastCommittedIndex = lastCommittedIndex;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
        // 触发 状态机的commited 只有首次超过半数时触发
        // 相当于是通过stateMachine 转发给node 提交任务之后通过ballotBox 在确认票数超过半数时 回调node的caller 对象
        // 代表本次提交成功了
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
     * 只能当某个候选者变成ledaer 时调用  这里只是更新 悬置的index
     * @param newPendingIndex pending index of new leader
     * @return returns true if reset success
     */
    public boolean resetPendingIndex(final long newPendingIndex) {
        final long stamp = this.stampedLock.writeLock();
        try {
            // 如果 悬置index 不为空 或者 悬置队列不为空 返回false 啥意思???
            // 调用该方法前 必须确保 属性为初始状态
            if (!(this.pendingIndex == 0 && this.pendingMetaQueue.isEmpty())) {
                LOG.error("resetPendingIndex fail, pendingIndex={}, pendingMetaQueueSize={}.", this.pendingIndex,
                    this.pendingMetaQueue.size());
                return false;
            }
            // 新的悬置index 小于最后提交index 也返回false
            if (newPendingIndex <= this.lastCommittedIndex) {
                LOG.error("resetPendingIndex fail, newPendingIndex={}, lastCommittedIndex={}.", newPendingIndex,
                    this.lastCommittedIndex);
                return false;
            }
            // 更新index
            this.pendingIndex = newPendingIndex;
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
     * 该方法的调用场景是 当向一个node提交任务时  会尝试向其他所有节点提交 当超过半数成功后才会返回成功
     * 当 node.apply 传入了多个 LogEntry 时 会创建多个 ballot对象 每个对象都对应整个集群中的投票动作
     * @param conf      current configuration   一个集群
     * @param oldConf   old configuration       旧集群节点
     * @param done      callback                处理完成后的回调方法
     * @return          returns true on success 判断添加是否成功
     */
    public boolean appendPendingTask(final Configuration conf, final Configuration oldConf, final Closure done) {
        // 生成"票"
        final Ballot bl = new Ballot();
        // ballot 对象内部 包含需要访问的所有节点
        if (!bl.init(conf, oldConf)) {
            LOG.error("Fail to init ballot.");
            return false;
        }
        // 改进的写锁
        final long stamp = this.stampedLock.writeLock();
        try {
            // 确保pendingIndex 大于 0 默认值应该就是0   TODO 先假设会正常添加吧 这个值难道会在哪个地方被设置么???
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
