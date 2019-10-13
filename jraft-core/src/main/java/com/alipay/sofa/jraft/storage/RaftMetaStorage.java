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
package com.alipay.sofa.jraft.storage;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;

/**
 * Raft metadata storage service.
 * 元数据存储
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:54:21 PM
 */
public interface RaftMetaStorage extends Lifecycle<RaftMetaStorageOptions>, Storage {

    /**
     * Set current term.
     * 设置当前任期
     */
    boolean setTerm(final long term);

    /**
     * Get current term.
     * 获取任期
     */
    long getTerm();

    /**
     * Set voted for information.
     * 代表决定投给哪个节点
     */
    boolean setVotedFor(final PeerId peerId);

    /**
     * Get voted for information.
     * 获取投票信息
     */
    PeerId getVotedFor();

    /**
     * Set term and voted for information.
     * 设置投票的节点和对应的任期
     */
    boolean setTermAndVotedFor(final long term, final PeerId peerId);
}
