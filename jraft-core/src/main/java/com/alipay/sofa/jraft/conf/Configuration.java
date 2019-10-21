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
package com.alipay.sofa.jraft.conf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Copiable;

/**
 * A configuration with a set of peers.
 * @author boyan (boyan@alibaba-inc.com)
 * 配置代表某一时期整个集群中有效的节点
 * 2018-Mar-15 11:00:26 AM
 */
public class Configuration implements Iterable<PeerId>, Copiable<Configuration> {

    /**
     * 内部携带的一组 参与者   PeerId 代表 ip + port
     */
    private List<PeerId> peers = new ArrayList<>();

    public Configuration() {
        super();
    }

    /**
     * 初始化就是将 传入的参与者设置到peer中 注意这里使用了 深拷贝
     * @param conf
     */
    public Configuration(final Iterable<PeerId> conf) {
        for (final PeerId peer : conf) {
            this.peers.add(peer.copy());
        }
    }

    @Override
    public Configuration copy() {
        return new Configuration(this.peers);
    }

    /**
     * 清除候选者
     */
    public void reset() {
        this.peers.clear();
    }

    /**
     * 判断该对象的 候选者是否为空
     * @return
     */
    public boolean isEmpty() {
        return this.peers.isEmpty();
    }

    public int size() {
        return this.peers.size();
    }

    @Override
    public Iterator<PeerId> iterator() {
        return this.peers.iterator();
    }

    public Set<PeerId> getPeerSet() {
        return new HashSet<>(this.peers);
    }

    public List<PeerId> listPeers() {
        return new ArrayList<>(this.peers);
    }

    public List<PeerId> getPeers() {
        return this.peers;
    }

    /**
     * 将候选人 替换成传入的值
     * @param peers
     */
    public void setPeers(final List<PeerId> peers) {
        this.peers.clear();
        for (final PeerId peer : peers) {
            this.peers.add(peer.copy());
        }
    }

    public void appendPeers(final Collection<PeerId> set) {
        this.peers.addAll(set);
    }

    public boolean addPeer(final PeerId peer) {
        return this.peers.add(peer);
    }

    public boolean removePeer(final PeerId peer) {
        return this.peers.remove(peer);
    }

    public boolean contains(final PeerId peer) {
        return this.peers.contains(peer);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.peers == null ? 0 : this.peers.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Configuration other = (Configuration) obj;
        if (this.peers == null) {
            return other.peers == null;
        } else {
            return this.peers.equals(other.peers);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final List<PeerId> peers = listPeers();
        int i = 0;
        final int size = peers.size();
        for (final PeerId peer : peers) {
            sb.append(peer);
            if (i < size - 1) {
                sb.append(",");
            }
            i++;
        }
        return sb.toString();
    }

    /**
     * 根据 配置 str 生成 configuration 对象
     * @param conf
     * @return
     */
    public boolean parse(final String conf) {
        if (conf == null) {
            return false;
        }
        reset();
        final String[] peerStrs = StringUtils.split(conf, ",");
        for (final String peerStr : peerStrs) {
            final PeerId peer = new PeerId();
            if (peer.parse(peerStr)) {
                addPeer(peer);
            }
        }
        return true;
    }

    /**
     *  Get the difference between |*this| and |rhs|
     *  |included| would be assigned to |*this| - |rhs|
     *  |excluded| would be assigned to |rhs| - |*this|
     *  获取本对象与 rhs 的 差集  included 代表本对象 - rhs  excluded 代表 rhs - 本对象
     */
    public void diff(final Configuration rhs, final Configuration included, final Configuration excluded) {
        included.peers = new ArrayList<>(this.peers);
        included.peers.removeAll(rhs.peers);
        excluded.peers = new ArrayList<>(rhs.peers);
        excluded.peers.removeAll(this.peers);
    }
}
