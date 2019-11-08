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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.util.ConcurrentHashSet;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReplicatorGroupOptions;
import com.alipay.sofa.jraft.option.ReplicatorOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadId;

/**
 * Replicator group for a raft group.
 * @author boyan (boyan@alibaba-inc.com)
 * 该对象管理了整个raft 的组内复制逻辑
 * 2018-Apr-04 1:54:51 PM
 */
public class ReplicatorGroupImpl implements ReplicatorGroup {

    private static final Logger                   LOG                = LoggerFactory
                                                                         .getLogger(ReplicatorGroupImpl.class);

    // <peerId, replicatorId>   key 标记地址 value 标识唯一复制机 (该对象内部会包含一个重入锁)
    private final ConcurrentMap<PeerId, ThreadId> replicatorMap      = new ConcurrentHashMap<>();
    /** common replicator options */
    private ReplicatorOptions                     commonOptions;
    private int                                   dynamicTimeoutMs   = -1;
    private int                                   electionTimeoutMs  = -1;
    private RaftOptions                           raftOptions;
    /**
     * 存放一组尝试连接失败的 节点
     */
    private final Set<PeerId>                     failureReplicators = new ConcurrentHashSet<>();

    @Override
    public boolean init(final NodeId nodeId, final ReplicatorGroupOptions opts) {
        this.dynamicTimeoutMs = opts.getHeartbeatTimeoutMs();
        this.electionTimeoutMs = opts.getElectionTimeoutMs();
        this.raftOptions = opts.getRaftOptions();
        // 将属性暂存到一个 commonOpt 中 因为每个Replicator 都是共用这些配置
        this.commonOptions = new ReplicatorOptions();
        this.commonOptions.setDynamicHeartBeatTimeoutMs(this.dynamicTimeoutMs);
        this.commonOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        // 这里包含了一个 client 对象
        this.commonOptions.setRaftRpcService(opts.getRaftRpcClientService());
        this.commonOptions.setLogManager(opts.getLogManager());
        // 每个复制机都能访问到 投票箱
        this.commonOptions.setBallotBox(opts.getBallotBox());
        this.commonOptions.setNode(opts.getNode());
        // 默认任期为0
        this.commonOptions.setTerm(0);
        this.commonOptions.setGroupId(nodeId.getGroupId());
        this.commonOptions.setServerId(nodeId.getPeerId());
        this.commonOptions.setSnapshotStorage(opts.getSnapshotStorage());
        this.commonOptions.setTimerManager(opts.getTimerManager());
        return true;
    }

    /**
     * 发送心跳请求
     * @param peer    target peer
     * @param closure callback
     */
    @Override
    public void sendHeartbeat(final PeerId peer, final RpcResponseClosure<AppendEntriesResponse> closure) {
        // 通过 peerId 找到对应的 复制者
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            // 触发异常回调
            if (closure != null) {
                closure.run(new Status(RaftError.EHOSTDOWN, "Peer %s is not connected", peer));
            }
            return;
        }
        Replicator.sendHeartbeat(rid, closure);
    }

    @Override
    public ThreadId getReplicator(final PeerId peer) {
        return this.replicatorMap.get(peer);
    }

    /**
     * 想要正常使用复制机对象 首先需要获取该组下所有的节点 并遍历调用 addReplicator 详见NodeImpl
     * 这里使用了无锁实现
     * @param peer target peer
     * @return
     */
    @Override
    public boolean addReplicator(final PeerId peer) {
        Requires.requireTrue(this.commonOptions.getTerm() != 0);
        if (this.replicatorMap.containsKey(peer)) {
            // 该列表中记录了添加失败的 peer 如果某个节点已经包含在replicatorMap中了 就从failureReplicators 中移除该节点
            this.failureReplicators.remove(peer);
            return true;
        }
        final ReplicatorOptions opts = this.commonOptions == null ? new ReplicatorOptions() : this.commonOptions.copy();

        opts.setPeerId(peer);
        // 初始化复制机对象
        final ThreadId rid = Replicator.start(opts, this.raftOptions);
        // 代表没有连接到某个 client
        if (rid == null) {
            LOG.error("Fail to start replicator to peer={}.", peer);
            this.failureReplicators.add(peer);
            return false;
        }
        return this.replicatorMap.put(peer, rid) == null;
    }

    /**
     * 清除 失败的复制者
     */
    @Override
    public void clearFailureReplicators() {
        this.failureReplicators.clear();
    }

    /**
     * 为整个复制组中所有的 复制机设置一个 追赶回调
     * @param peer
     * @param maxMargin
     * @param dueTime
     * @param done
     * @return
     */
    @Override
    public boolean waitCaughtUp(final PeerId peer, final long maxMargin, final long dueTime, final CatchUpClosure done) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return false;
        }

        Replicator.waitForCaughtUp(rid, maxMargin, dueTime, done);
        return true;
    }

    @Override
    public long getLastRpcSendTimestamp(final PeerId peer) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return 0L;
        }
        // 获取发送到该节点的最后时间戳 (收到响应才作数)
        return Replicator.getLastRpcSendTimestamp(rid);
    }

    @Override
    public boolean stopAll() {
        final List<ThreadId> rids = new ArrayList<>(this.replicatorMap.values());
        this.replicatorMap.clear();
        this.failureReplicators.clear();
        for (final ThreadId rid : rids) {
            Replicator.stop(rid);
        }
        return true;
    }

    /**
     * 检查目标节点的状态机是否正常
     * @param peer     peer of replicator
     * @param lockNode if lock with node
     */
    @Override
    public void checkReplicator(final PeerId peer, final boolean lockNode) {
        final ThreadId rid = this.replicatorMap.get(peer);
        // noinspection StatementWithEmptyBody
        if (rid == null) {
            // Create replicator if it's not found for leader.
            final NodeImpl node = this.commonOptions.getNode();
            if (lockNode) {
                node.writeLock.lock();
            }
            try {
                // 获取本节点 在确定本节点是leader 的前提下 创建对端节点 在调用 addReolicator时 还会发送心跳
                if (node.isLeader() && this.failureReplicators.contains(peer) && addReplicator(peer)) {
                    this.failureReplicators.remove(peer);
                }
            } finally {
                if (lockNode) {
                    node.writeLock.unlock();
                }
            }
        } else { // NOPMD
            // Unblock it right now.
            // Replicator.unBlockAndSendNow(rid);
        }
    }

    /**
     * 停止某个复制机 一般是 当前节点从leader 变为follower 时触发
     * @param peer the peer of replicator
     * @return
     */
    @Override
    public boolean stopReplicator(final PeerId peer) {
        LOG.info("Stop replicator to {}.", peer);
        this.failureReplicators.remove(peer);
        final ThreadId rid = this.replicatorMap.remove(peer);
        if (rid == null) {
            return false;
        }
        // Calling ReplicatorId.stop might lead to calling stopReplicator again,
        // erase entry first to avoid race condition
        return Replicator.stop(rid);
    }

    /**
     * 仅更新任期 需要配合 addReplicator
     * @param newTerm new term num
     * @return
     */
    @Override
    public boolean resetTerm(final long newTerm) {
        if (newTerm <= this.commonOptions.getTerm()) {
            return false;
        }
        this.commonOptions.setTerm(newTerm);
        return true;
    }

    @Override
    public boolean resetHeartbeatInterval(final int newIntervalMs) {
        this.dynamicTimeoutMs = newIntervalMs;
        return true;
    }

    @Override
    public boolean resetElectionTimeoutInterval(final int newIntervalMs) {
        this.electionTimeoutMs = newIntervalMs;
        return true;
    }

    @Override
    public boolean contains(final PeerId peer) {
        return this.replicatorMap.containsKey(peer);
    }

    @Override
    public boolean transferLeadershipTo(final PeerId peer, final long logIndex) {
        final ThreadId rid = this.replicatorMap.get(peer);
        return rid != null && Replicator.transferLeadership(rid, logIndex);
    }

    @Override
    public boolean stopTransferLeadership(final PeerId peer) {
        final ThreadId rid = this.replicatorMap.get(peer);
        return rid != null && Replicator.stopTransferLeadership(rid);
    }

    /**
     * 停止所有的复制机 并返回最有可能成为leader 的候选人
     * @param conf configuration of all replicators
     * @return
     */
    @Override
    public ThreadId stopAllAndFindTheNextCandidate(final ConfigurationEntry conf) {
        ThreadId candidate = null;
        // 找到写入数据最多的节点
        final PeerId candidateId = this.findTheNextCandidate(conf);
        if (candidateId != null) {
            candidate = this.replicatorMap.get(candidateId);
        } else {
            LOG.info("Fail to find the next candidate.");
        }
        // 关闭除候选外的其他复制机
        for (final ThreadId r : this.replicatorMap.values()) {
            if (r != candidate) {
                Replicator.stop(r);
            }
        }
        this.replicatorMap.clear();
        this.failureReplicators.clear();
        return candidate;
    }

    /**
     * 寻找最可能成为leader 的候选人
     * @param conf configuration of all replicators
     * @return
     */
    @Override
    public PeerId findTheNextCandidate(final ConfigurationEntry conf) {
        PeerId peerId = null;
        long maxIndex = -1L;
        for (final Map.Entry<PeerId, ThreadId> entry : this.replicatorMap.entrySet()) {
            // 不包含在 复制机中的跳过
            if (!conf.contains(entry.getKey())) {
                continue;
            }
            // 这里返回复制机的 nextIndex  该值代表 正确请求到对端并接收到响应的偏移量 那么该值最大就可以理解为写入的数据最多
            final long nextIndex = Replicator.getNextIndex(entry.getValue());
            if (nextIndex > maxIndex) {
                maxIndex = nextIndex;
                peerId = entry.getKey();
            }
        }

        if (maxIndex == -1L) {
            return null;
        } else {
            return peerId;
        }
    }

    @Override
    public List<ThreadId> listReplicators() {
        return new ArrayList<>(this.replicatorMap.values());
    }

    @Override
    public void describe(final Printer out) {
        out.print("  replicators: ") //
            .println(this.replicatorMap.values());
        out.print("  failureReplicators: ") //
            .println(this.failureReplicators);
    }
}
