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

import java.util.LinkedList;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.Requires;

/**
 * Configuration manager
 * 绑定在某个node 上 一个node 对应一个 logManager 一个logManager 对应一个confManager
 * 该对象内部绑定了所有更改历史的快照 (不是那个写入的快照)
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Apr-04 2:24:54 PM
 */
public class ConfigurationManager {

    private static final Logger                  LOG            = LoggerFactory.getLogger(ConfigurationManager.class);

    /**
     * 内部以链表形式维护 老集群/新集群
     */
    private final LinkedList<ConfigurationEntry> configurations = new LinkedList<>();
    /**
     * 最近一次快照对应 集群信息
     */
    private ConfigurationEntry                   snapshot       = new ConfigurationEntry();

    /**
     * Adds a new conf entry.
     * 为manager 增加 配置Entry 对象   新增的对象 index 必须大于最后一个 conf  在jraft中 数据有新旧的概念 只有新数据可以覆盖
     */
    public boolean add(final ConfigurationEntry entry) {
        if (!this.configurations.isEmpty()) {
            if (this.configurations.peekLast().getId().getIndex() >= entry.getId().getIndex()) {
                LOG.error("Did you forget to call truncateSuffix before the last log index goes back.");
                return false;
            }
        }
        // 往链表中插入数据
        return this.configurations.add(entry);
    }

    /**
     * [1, first_index_kept) are being discarded
     * 删除指定index 前的数据 很可能就是 leader 写入的数据没有提交 之后更换了leader 将旧leader 写入失败的数据清除
     */
    public void truncatePrefix(final long firstIndexKept) {
        while (!this.configurations.isEmpty() && this.configurations.peekFirst().getId().getIndex() < firstIndexKept) {
            this.configurations.pollFirst();
        }
    }

    /**
     * (last_index_kept, infinity) are being discarded
     * 删除指定偏移量后的数据
     */
    public void truncateSuffix(final long lastIndexKept) {
        while (!this.configurations.isEmpty() && this.configurations.peekLast().getId().getIndex() > lastIndexKept) {
            this.configurations.pollLast();
        }
    }

    public ConfigurationEntry getSnapshot() {
        return this.snapshot;
    }

    /**
     * 设置快照
     * @param snapshot
     */
    public void setSnapshot(final ConfigurationEntry snapshot) {
        this.snapshot = snapshot;
    }

    /**
     * 获取最后一个配置  优先从链表中获取 如果链表不存在就返回快照对象
     * @return
     */
    public ConfigurationEntry getLastConfiguration() {
        if (this.configurations.isEmpty()) {
            return snapshot;
        } else {
            return this.configurations.peekLast();
        }
    }

    /**
     * 获取指定偏移量的 confEntry 对象
     * @param lastIncludedIndex
     * @return
     */
    public ConfigurationEntry get(final long lastIncludedIndex) {
        if (this.configurations.isEmpty()) {
            Requires.requireTrue(lastIncludedIndex >= this.snapshot.getId().getIndex(),
                "lastIncludedIndex %d is less than snapshot index %d", lastIncludedIndex, this.snapshot.getId()
                    .getIndex());
            // 该快照对象是什么时候生成的???
            return this.snapshot;
        }
        ListIterator<ConfigurationEntry> it = this.configurations.listIterator();
        while (it.hasNext()) {
            if (it.next().getId().getIndex() > lastIncludedIndex) {
                // 此时的  confEntry 应该是      start                     |------   lastIncludedIndex  ------|
                it.previous();                                //         start                              end
                break;
            }
        }
        // 获取的 confEntry 刚好小于 lastIncludedIndex  这里调用2次 it.previous() 是不是搞错了???
        if (it.hasPrevious()) {
            // find the first position that is less than or equal to lastIncludedIndex.
            return it.previous();
        } else {
            // position not found position, return snapshot.
            return this.snapshot;
        }
    }
}
