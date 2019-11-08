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
package com.alipay.sofa.jraft;

import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.util.Describer;

/**
 * Finite state machine caller.
 * 状态机调用者
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 11:07:52 AM
 */
public interface FSMCaller extends Lifecycle<FSMCallerOptions>, Describer {

    /**
     * Listen on lastAppliedLogIndex update events.
     * 监听 lastAppliedLogIndex 发生变化时的回调
     * @author dennis
     */
    interface LastAppliedLogIndexListener {

        /**
         * Called when lastAppliedLogIndex updated.
         *
         * @param lastAppliedLogIndex the log index of last applied
         */
        void onApplied(final long lastAppliedLogIndex);
    }

    /**
     * Adds a LastAppliedLogIndexListener.
     * 增加一个当 lastAppliedLogIndex 发生变化时的监听器
     */
    void addLastAppliedLogIndexListener(final LastAppliedLogIndexListener listener);

    /**
     * Called when log entry committed
     * 当 一组logEntry 写入到超过半数的LogManager中 后触发
     * @param committedIndex committed log indexx
     */
    boolean onCommitted(final long committedIndex);

    /**
     * Called after loading snapshot.
     * 当成功从leader处拉取完数据后触发  又或者是首次启动发现本地有快照文件 就代表可以通过快照文件进行初始化
     * @param done callback
     */
    boolean onSnapshotLoad(final LoadSnapshotClosure done);

    /**
     * Called after saving snapshot.
     * 开始保存快照
     * @param done callback
     */
    boolean onSnapshotSave(final SaveSnapshotClosure done);

    /**
     * Called when the leader stops.
     *
     * @param status status info
     */
    boolean onLeaderStop(final Status status);

    /**
     * Called when the leader starts.
     *
     * @param term current term
     */
    boolean onLeaderStart(final long term);

    /**
     * Called when start following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStartFollowing(final LeaderChangeContext ctx);

    /**
     * Called when stop following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStopFollowing(final LeaderChangeContext ctx);

    /**
     * Called when error happens.
     * 当写入数据时发生异常 比如写入ConfManager 失败
     * @param error error info
     */
    boolean onError(final RaftException error);

    /**
     * Returns the last log entry index to apply state machine.
     */
    long getLastAppliedIndex();

    /**
     * Called after shutdown to wait it terminates.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;
}
