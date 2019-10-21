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
package com.alipay.sofa.jraft.entity;

import java.util.ArrayList;
import java.util.List;

import com.alipay.sofa.jraft.conf.Configuration;

/**
 * A ballot to vote.
 * 投票箱   应该是所有节点共用一个投票箱 里面还维护了一共需要多少票能竞选成功
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-15 2:29:11 PM
 */
public class Ballot {

    /**
     * 查找PeerId 的 线索对象
     */
    public static final class PosHint {
        /**
         * 对应 peerIdList
         */
        int pos0 = -1; // position in current peers
        /**
         * 对应 oldPeerIdList
         */
        int pos1 = -1; // position in old peers
    }

    /**
     * 未找到的节点
     */
    public static class UnfoundPeerId {
        /**
         * 标识该节点对象
         */
        PeerId  peerId;
        /**
         * 是否找到
         */
        boolean found;
        /**
         * 记录该UnfoundPeerId 的下标
         */
        int     index;

        public UnfoundPeerId(PeerId peerId, int index, boolean found) {
            super();
            this.peerId = peerId;
            this.index = index;
            this.found = found;
        }
    }

    /**
     * 存放一组 未找到的节点
     */
    private final List<UnfoundPeerId> peers    = new ArrayList<>();
    /**
     * 法定人数 (有效投票人数???)
     */
    private int                       quorum;
    private final List<UnfoundPeerId> oldPeers = new ArrayList<>();
    private int                       oldQuorum;

    /**
     * Init the ballot with current conf and old conf.
     * 通过旧的集群节点和新的集群节点来初始化 投票对象
     * @param conf      current configuration
     * @param oldConf   old configuration
     * @return true if init success
     */
    public boolean init(Configuration conf, Configuration oldConf) {
        // 清除 Ballot的 旧数据
        this.peers.clear();
        this.oldPeers.clear();
        quorum = oldQuorum = 0;
        int index = 0;
        if (conf != null) {
            // 初始化一组未找到的 peerId 对象
            for (PeerId peer : conf) {
                this.peers.add(new UnfoundPeerId(peer, index++, false));
            }
        }

        // 计算选举成功需要的人数 不是说要半数以上吗这里可能会计算出半数的值
        this.quorum = this.peers.size() / 2 + 1;
        if (oldConf == null) {
            return true;
        }
        index = 0;
        for (PeerId peer : oldConf) {
            this.oldPeers.add(new UnfoundPeerId(peer, index++, false));
        }

        // 计算旧集群的投票成功人数
        this.oldQuorum = this.oldPeers.size() / 2 + 1;
        return true;
    }

    /**
     * 从列表中找到某个 Peer
     * @param peerId
     * @param peers
     * @param posHint
     * @return
     */
    private UnfoundPeerId findPeer(PeerId peerId, List<UnfoundPeerId> peers, int posHint) {
        // 当线索非法 或者 获取的对象与给定的peerId 不符合时 挨个查找
        if (posHint < 0 || posHint >= peers.size() || !peers.get(posHint).peerId.equals(peerId)) {
            for (UnfoundPeerId ufp : peers) {
                if (ufp.peerId.equals(peerId)) {
                    return ufp;
                }
            }
            return null;
        }

        // 线索对象 合法时 直接使用该值作为下标获取Peer
        return peers.get(posHint);
    }

    /**
     * 为给定的节点投票
     * @param peerId
     * @param hint  大多数情况 hint 是一个无效的值 之后通过找到某个 peerId 然后赋值到hint上
     * @return
     */
    public PosHint grant(PeerId peerId, PosHint hint) {
        // 找到给定的 PeerId
        UnfoundPeerId peer = findPeer(peerId, peers, hint.pos0);
        // TODO 这样看来不是 不能为一个 peer 投票2次了吗 看来理解还有偏差
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                // 啥意思???
                this.quorum--;
            }
            // index 就是该元素的下标 这样设置有什么意义吗???
            hint.pos0 = peer.index;
        } else {
            hint.pos0 = -1;
        }
        if (oldPeers.isEmpty()) {
            hint.pos1 = -1;
            return hint;
        }
        // 基本同上
        peer = findPeer(peerId, oldPeers, hint.pos1);
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                oldQuorum--;
            }
            hint.pos1 = peer.index;
        } else {
            hint.pos1 = -1;
        }

        return hint;
    }

    public void grant(PeerId peerId) {
        this.grant(peerId, new PosHint());
    }

    /**
     * Returns true when the ballot is granted.
     * 代表该投票箱 投完了全部的票
     * @return true if the ballot is granted
     */
    public boolean isGranted() {
        return this.quorum <= 0 && oldQuorum <= 0;
    }
}
