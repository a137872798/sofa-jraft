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

import java.util.HashSet;
import java.util.Set;

import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;

/**
 * A configuration entry with current peers and old peers.
 * @author boyan (boyan@alibaba-inc.com)
 * 配置实体 内部维护了旧的集群节点 和新的集群节点  在重新发起选举时起作用 不让old节点参与新一轮的投票
 * 2018-Apr-04 2:25:06 PM
 */
public class ConfigurationEntry {

    /**
     * 这个值有什么用 ???
     */
    private LogId         id      = new LogId(0, 0);
    /**
     * 当前集群节点
     */
    private Configuration conf    = new Configuration();
    /**
     * 旧集群节点
     */
    private Configuration oldConf = new Configuration();

    public LogId getId() {
        return this.id;
    }

    public void setId(LogId id) {
        this.id = id;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getOldConf() {
        return this.oldConf;
    }

    public void setOldConf(Configuration oldConf) {
        this.oldConf = oldConf;
    }

    public ConfigurationEntry() {
        super();
    }

    public ConfigurationEntry(LogId id, Configuration conf, Configuration oldConf) {
        super();
        this.id = id;
        this.conf = conf;
        this.oldConf = oldConf;
    }

    /**
     * 是否稳定 如果 不存在旧的集群 代表集群本身没有变化过
     * @return
     */
    public boolean isStable() {
        return this.oldConf.isEmpty();
    }

    public boolean isEmpty() {
        return this.conf.isEmpty();
    }

    /**
     * 返回所有 peer
     * @return
     */
    public Set<PeerId> listPeers() {
        final Set<PeerId> ret = new HashSet<>(this.conf.listPeers());
        ret.addAll(this.oldConf.listPeers());
        return ret;
    }

    public boolean contains(PeerId peer) {
        return this.conf.contains(peer) || this.oldConf.contains(peer);
    }

    @Override
    public String toString() {
        return "ConfigurationEntry [id=" + this.id + ", conf=" + this.conf + ", oldConf=" + this.oldConf + "]";
    }
}
