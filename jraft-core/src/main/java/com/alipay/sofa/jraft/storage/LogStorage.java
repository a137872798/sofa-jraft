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

import java.util.List;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.LogStorageOptions;

/**
 * Log entry storage service.
 * 日志存储接口  用于记录 raft配置变更和用户提交任务的日志  以及具备将日志信息 同步到其他节点的能力
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:43:54 PM
 */
public interface LogStorage extends Lifecycle<LogStorageOptions>, Storage {

    /**
     * Returns first log index in log.
     * 获取第一个日志的 偏移量
     */
    long getFirstLogIndex();

    /**
     * Returns last log index in log.
     * 获取最后一个日志的 偏移量
     */
    long getLastLogIndex();

    /**
     * Get logEntry by index.
     * 根据偏移量 获取某块日志 看来日志是以 LogEntry 为 单位进行存储的
     */
    LogEntry getEntry(final long index);

    /**
     * Get logEntry's term by index. This method is deprecated, you should use {@link #getEntry(long)} to get the log id's term.
     * @deprecated
     */
    @Deprecated
    long getTerm(final long index);

    /**
     * Append entries to log.
     * 该对象应该是 类似于 RocketMq 的 commitLog 吧   将某个logEntry 追加到 logStorage
     */
    boolean appendEntry(final LogEntry entry);

    /**
     * Append entries to log, return append success number.
     * 将一组日志实体追加到 LogStorage 上
     */
    int appendEntries(final List<LogEntry> entries);

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will
     * be discarded.
     * 丢弃 从最早的偏移量到 给定的  偏移量的数据
     */
    boolean truncatePrefix(final long firstIndexKept);

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index]
     * will be discarded.
     * 删除未提交的数据 也就是传入的偏移量 到末尾的 偏移量    (未提交就是指接受到用户请求后日志没有成功发送到半数以上的follower)
     */
    boolean truncateSuffix(final long lastIndexKept);

    /**
     * Drop all the existing logs and reset next log index to |next_log_index|.
     * This function is called after installing snapshot from leader.
     * 丢弃所有日志 并将偏移量定位到 给定的位置
     */
    boolean reset(final long nextLogIndex);
}
