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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.JRaftException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.ChangePeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.ChangePeersResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.RemovePeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.RemovePeerResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.SnapshotRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.TransferLeaderRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Cli service implementation.
 * 命令行交互 service 内部包含 增/减 节点 更换leader 等操作
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:12:06 PM
 */
public class CliServiceImpl implements CliService {

    private static final Logger LOG = LoggerFactory.getLogger(CliServiceImpl.class);

    private CliOptions          cliOptions;
    /**
     * 监听命令行的客户端
     */
    private CliClientService    cliClientService;

    @Override
    public synchronized boolean init(final CliOptions opts) {
        Requires.requireNonNull(opts, "Null cli options");

        if (this.cliClientService != null) {
            return true;
        }
        this.cliOptions = opts;
        this.cliClientService = new BoltCliClientService();
        return this.cliClientService.init(this.cliOptions);
    }

    /**
     * 清空设置的对象
     */
    @Override
    public synchronized void shutdown() {
        if (this.cliClientService == null) {
            return;
        }
        this.cliClientService.shutdown();
        this.cliClientService = null;
    }

    /**
     * 为 group 增加新的节点
     * @param groupId the raft group id
     * @param conf    current configuration    当前配置是从哪里获取的 ???
     * @param peer    peer to add
     * @return
     */
    @Override
    public Status addPeer(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");

        final PeerId leaderId = new PeerId();
        // 通过 发送获取leader的请求到每个 conf中 将结果设置到leaderId 中
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }
        final AddPeerRequest.Builder rb = AddPeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setPeerId(peer.toString());

        try {
            // 往leader 上发送 添加节点的请求   返回结果 对应 AddPeerResponse  内部 包含添加前快照 (通过node.listPeers 获取) 并将添加后的新快照也返回 这里指的快照是集群节点快照
            // 而不是 snapshot 在 jraft 中 snapshot 有其他含义
            final Message result = this.cliClientService.addPeer(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof AddPeerResponse) {
                final AddPeerResponse resp = (AddPeerResponse) result;
                // 获取旧集群信息
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                // 生成 oldConf 和 conf 仅仅为打印日志
                LOG.info("Configuration of replication group {} changed from {} to {}.", groupId, oldConf, newConf);
                return Status.OK();
            } else {
                return statusFromResponse(result);
            }

        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    /**
     * 从res 对象中 抽取 status
     * @param result
     * @return
     */
    private Status statusFromResponse(final Message result) {
        final ErrorResponse resp = (ErrorResponse) result;
        return new Status(resp.getErrorCode(), resp.getErrorMsg());
    }

    /**
     * 将某个节点从 某个组中移除
     * @param groupId the raft group id
     * @param conf    current configuration
     * @param peer    peer to remove
     * @return
     */
    @Override
    public Status removePeer(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");
        Requires.requireTrue(!peer.isEmpty(), "Removing peer is blank");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final RemovePeerRequest.Builder rb = RemovePeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setPeerId(peer.toString());

        // 套路同上  removePeer 最终都是委托给 node 对象
        try {
            final Message result = this.cliClientService.removePeer(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof RemovePeerResponse) {
                final RemovePeerResponse resp = (RemovePeerResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}", groupId, oldConf, newConf);
                return Status.OK();
            } else {
                return statusFromResponse(result);

            }
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    /**
     * 将当前 conf 变成 newPeers
     * @param groupId  the raft group id
     * @param conf     current configuration
     * @param newPeers new peers to change
     * @return
     */
    // TODO refactor addPeer/removePeer/changePeers/transferLeader, remove duplicated code.
    @Override
    public Status changePeers(final String groupId, final Configuration conf, final Configuration newPeers) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(newPeers, "Null new peers");

        // 先找到leader 节点 因为只有该节点具备修改集群的能力
        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final ChangePeersRequest.Builder rb = ChangePeersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        // 将 newConf 信息设置到 req中
        for (final PeerId peer : newPeers) {
            rb.addNewPeers(peer.toString());
        }

        try {
            final Message result = this.cliClientService.changePeers(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof ChangePeersResponse) {
                final ChangePeersResponse resp = (ChangePeersResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}", groupId, oldConf, newConf);
                return Status.OK();
            } else {
                return statusFromResponse(result);

            }
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    /**
     * 重置节点 和上面有啥区别吗   看来区别点在 node.changePeer 和 node.resetPeer
     * @param groupId  the raft group id
     * @param peerId
     * @param newPeers new peers to reset
     * @return
     */
    @Override
    public Status resetPeer(final String groupId, final PeerId peerId, final Configuration newPeers) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(peerId, "Null peerId");
        Requires.requireNonNull(newPeers, "Null new peers");

        if (!this.cliClientService.connect(peerId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to %s", peerId);
        }

        final ResetPeerRequest.Builder rb = ResetPeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setPeerId(peerId.toString());
        for (final PeerId peer : newPeers) {
            rb.addNewPeers(peer.toString());
        }

        try {
            final Message result = this.cliClientService.resetPeer(peerId.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    /**
     * 更换leader
     * @param groupId the raft group id
     * @param conf    current configuration
     * @param peer    target peer of new leader
     * @return
     */
    @Override
    public Status transferLeader(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");

        final PeerId leaderId = new PeerId();
        // 从conf 中 的peer 中 挨个发送获取leader 的请求 如果找到了 将leader 信息设置到 leaderId对象中
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final TransferLeaderRequest.Builder rb = TransferLeaderRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        if (!peer.isEmpty()) {
            rb.setPeerId(peer.toString());
        }

        try {
            final Message result = this.cliClientService.transferLeader(leaderId.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status snapshot(final String groupId, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(peer, "Null peer");

        if (!this.cliClientService.connect(peer.getEndpoint())) {
            return new Status(-1, "Fail to init channel to %s", peer);
        }

        final SnapshotRequest.Builder rb = SnapshotRequest.newBuilder() //
            .setGroupId(groupId) //
            .setPeerId(peer.toString());

        try {
            final Message result = this.cliClientService.snapshot(peer.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    /**
     * 获取 leader 信息
     * @param groupId  the raft group id  组id
     * @param conf     configuration
     * @param leaderId id of leader  leader id
     * @return
     */
    @Override
    public Status getLeader(final String groupId, final Configuration conf, final PeerId leaderId) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(leaderId, "Null leader id");

        if (conf == null || conf.isEmpty()) {
            return new Status(RaftError.EINVAL, "Empty group configuration");
        }

        final Status st = new Status(-1, "Fail to get leader of group %s", groupId);
        // 发送getLeader 请求
        for (final PeerId peer : conf) {
            if (!this.cliClientService.connect(peer.getEndpoint())) {
                LOG.error("Fail to connect peer {} to get leader for group {}.", peer, groupId);
                continue;
            }

            final GetLeaderRequest.Builder rb = GetLeaderRequest.newBuilder() //
                .setGroupId(groupId) //
                .setPeerId(peer.toString());

            final Future<Message> result = this.cliClientService.getLeader(peer.getEndpoint(), rb.build(), null);
            try {

                final Message msg = result.get(
                    this.cliOptions.getTimeoutMs() <= 0 ? this.cliOptions.getRpcDefaultTimeout() : this.cliOptions
                        .getTimeoutMs(), TimeUnit.MILLISECONDS);
                if (msg instanceof ErrorResponse) {
                    if (st.isOk()) {
                        st.setError(-1, ((ErrorResponse) msg).getErrorMsg());
                    } else {
                        final String savedMsg = st.getErrorMsg();
                        st.setError(-1, "%s, %s", savedMsg, ((ErrorResponse) msg).getErrorMsg());
                    }
                } else {
                    final GetLeaderResponse response = (GetLeaderResponse) msg;
                    // 如果解析成功返回
                    if (leaderId.parse(response.getLeaderId())) {
                        break;
                    }
                }
            } catch (final Exception e) {
                if (st.isOk()) {
                    st.setError(-1, e.getMessage());
                } else {
                    final String savedMsg = st.getErrorMsg();
                    st.setError(-1, "%s, %s", savedMsg, e.getMessage());
                }
            }
        }

        if (leaderId.isEmpty()) {
            return st;
        }
        return Status.OK();
    }

    @Override
    public List<PeerId> getPeers(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, false);
    }

    @Override
    public List<PeerId> getAlivePeers(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, true);
    }

    /**
     * 应该是将 group 下的peer 重新分配吧 避免过多或过少
     * TODO 这个方法还不清楚是做什么的 等收集更多信息后再看
     * @param balanceGroupIds   all raft group ids to balance  需要重新分配的groupId
     * @param conf              configuration of all nodes   该 conf 中包含了所有组的 node 对象
     * @param rebalancedLeaderIds 存放 <groupId, LeaderId> 的键值对
     * @return
     */
    @Override
    public Status rebalance(final Set<String> balanceGroupIds, final Configuration conf,
                            final Map<String, PeerId> rebalancedLeaderIds) {
        Requires.requireNonNull(balanceGroupIds, "Null balance group ids");
        Requires.requireTrue(!balanceGroupIds.isEmpty(), "Empty balance group ids");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireTrue(!conf.isEmpty(), "No peers of configuration");

        LOG.info("Rebalance start with raft groups={}.", balanceGroupIds);

        final long start = Utils.monotonicMs();
        int transfers = 0;
        Status failedStatus = null;
        // 生成一个双端队列
        final Queue<String> groupDeque = new ArrayDeque<>(balanceGroupIds);
        final LeaderCounter leaderCounter = new LeaderCounter(balanceGroupIds.size(), conf.size());
        for (;;) {
            final String groupId = groupDeque.poll();
            if (groupId == null) { // well done
                break;
            }

            // 从某个group 中找到 leaderId
            final PeerId leaderId = new PeerId();
            final Status leaderStatus = getLeader(groupId, conf, leaderId);
            if (!leaderStatus.isOk()) {
                failedStatus = leaderStatus;
                break;
            }

            if (rebalancedLeaderIds != null) {
                rebalancedLeaderIds.put(groupId, leaderId);
            }

            // 小于平均数 就不需要处理了 当超过平均数 就要想办法移动到其他组
            if (leaderCounter.incrementAndGet(leaderId) <= leaderCounter.getExpectedAverage()) {
                // The num of leaders is less than the expected average, we are going to deal with others
                continue;
            }

            // Find the target peer and try to transfer the leader to this peer
            // 寻找应该转移的 目标节点  注意该节点是一个普通节点
            final PeerId targetPeer = findTargetPeer(leaderId, groupId, conf, leaderCounter);
            if (!targetPeer.isEmpty()) {
                // 将leader 转换成目标节点
                final Status transferStatus = transferLeader(groupId, conf, targetPeer);
                transfers++;
                if (!transferStatus.isOk()) {
                    // The failure of `transfer leader` usually means the node is busy,
                    // so we return failure status and should try `rebalance` again later.
                    failedStatus = transferStatus;
                    break;
                }

                LOG.info("Group {} transfer leader to {}.", groupId, targetPeer);
                leaderCounter.decrementAndGet(leaderId);
                groupDeque.add(groupId);
                if (rebalancedLeaderIds != null) {
                    rebalancedLeaderIds.put(groupId, targetPeer);
                }
            }
        }

        final Status status = failedStatus != null ? failedStatus : Status.OK();
        if (LOG.isInfoEnabled()) {
            LOG.info(
                "Rebalanced raft groups={}, status={}, number of transfers={}, elapsed time={} ms, rebalanced result={}.",
                balanceGroupIds, status, transfers, Utils.monotonicMs() - start, rebalancedLeaderIds);
        }
        return status;
    }

    /**
     * 寻找应该转移的目标节点
     * @param self  当前conf 由哪个 leader 来管理
     * @param groupId  该leader 所在 group
     * @param conf  所有node 的快照
     * @param leaderCounter  当前leader 管理的节点数量 一般是超过了平均值才会进入这里
     * @return
     */
    private PeerId findTargetPeer(final PeerId self, final String groupId, final Configuration conf,
                                  final LeaderCounter leaderCounter) {
        for (final PeerId peerId : getAlivePeers(groupId, conf)) {
            if (peerId.equals(self)) {
                continue;
            }
            // 这里会返回一个普通节点
            if (leaderCounter.get(peerId) >= leaderCounter.getExpectedAverage()) {
                continue;
            }
            return peerId;
        }
        return PeerId.emptyPeer();
    }

    /**
     * 获取指定组下 所有的 conf
     * @param groupId
     * @param conf
     * @param onlyGetAlive  是否只获取存活的节点
     * @return
     */
    private List<PeerId> getPeers(final String groupId, final Configuration conf, final boolean onlyGetAlive) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null conf");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            throw new IllegalStateException(st.getErrorMsg());
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            throw new IllegalStateException("Fail to init channel to leader " + leaderId);
        }

        final GetPeersRequest.Builder rb = GetPeersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setOnlyAlive(onlyGetAlive);

        try {
            final Message result = this.cliClientService.getPeers(leaderId.getEndpoint(), rb.build(), null).get(
                this.cliOptions.getTimeoutMs() <= 0 ? this.cliOptions.getRpcDefaultTimeout()
                    : this.cliOptions.getTimeoutMs(), TimeUnit.MILLISECONDS);
            if (result instanceof GetPeersResponse) {
                final GetPeersResponse resp = (GetPeersResponse) result;
                final List<PeerId> peerIdList = new ArrayList<>();
                for (final String peerIdStr : resp.getPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    peerIdList.add(newPeer);
                }
                return peerIdList;
            } else {
                final ErrorResponse resp = (ErrorResponse) result;
                throw new JRaftException(resp.getErrorMsg());
            }
        } catch (final JRaftException e) {
            throw e;
        } catch (final Exception e) {
            throw new JRaftException(e);
        }
    }

    public CliClientService getCliClientService() {
        return cliClientService;
    }

    /**
     * leader 计数器
     */
    private static class LeaderCounter {

        /**
         * key 为 leaderId value 为 该leader 管理的 节点数 该对象是配合 rebalance 方法 尽可能将 节点平均分配
         */
        private final Map<PeerId, Integer> counter = new HashMap<>();
        // The expected average leader number on every peerId
        // 每个 group 应该有多少 peer
        private final int                  expectedAverage;

        public LeaderCounter(final int groupCount, final int peerCount) {
            this.expectedAverage = (int) Math.ceil((double) groupCount / peerCount);
        }

        public int getExpectedAverage() {
            return expectedAverage;
        }

        public int incrementAndGet(final PeerId peerId) {
            return this.counter.compute(peerId, (ignored, num) -> num == null ? 1 : num + 1);
        }

        public int decrementAndGet(final PeerId peerId) {
            return this.counter.compute(peerId, (ignored, num) -> num == null ? 0 : num - 1);
        }

        public int get(final PeerId peerId) {
            return this.counter.getOrDefault(peerId, 0);
        }
    }
}
