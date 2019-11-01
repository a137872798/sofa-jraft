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

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * |StateMachine| is the sink of all the events of a very raft node.
 * Implement a specific StateMachine for your own business logic.
 * NOTE: All the interfaces are not guaranteed to be thread safe and they are
 * called sequentially, saying that every single operation will block all the
 * following ones.
 * 状态机接口  处在jraft 的 服务端上 处理核心逻辑  它运行在每个jraft node上
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 5:43:21 PM
 */
public interface StateMachine {

    /**
     * Update the StateMachine with a batch a tasks that can be accessed
     * through |iterator|.
     *
     * Invoked when one or more tasks that were passed to Node#apply(Task) have been
     * committed to the raft group (quorum of the group peers have received
     * those tasks and stored them on the backing storage).
     *
     * Once this function returns to the caller, we will regard all the iterated
     * tasks through |iter| have been successfully applied. And if you didn't
     * apply all the the given tasks, we would ronSnapshotSaveegard this as a critical error
     * and report a error whose type is ERROR_TYPE_STATE_MACHINE.
     * 用户提交的 task 往往是通过 累加 并堆积成一个迭代器后提交到 状态机的 这里代表批量处理任务
     * 当该方法返回的时候 代表所有任务都已经成功处理 如果出现了一个异常 则会触发 onError 方法
     * @param iter iterator of states
     */
    void onApply(final Iterator iter);

    /**
     * Invoked once when the raft node was shut down.
     * Default do nothing
     * 当某个node 关闭时触发
     */
    void onShutdown();

    /**
     * User defined snapshot generate function, this method will block StateMachine#onApply(Iterator).
     * user can make snapshot async when fsm can be cow(copy-on-write).
     * call done.run(status) when snapshot finished.
     * Default: Save nothing and returns error.
     * 保存快照 并触发对应的回调
     * @param writer snapshot writer   该对象代表写入快照的输入流
     * @param done   callback
     */
    void onSnapshotSave(final SnapshotWriter writer, final Closure done);

    /**
     * User defined snapshot load function
     * get and load snapshot
     * Default: Load nothing and returns error.
     * 使用指定的reader 去加载快照
     * @param reader snapshot reader   读取快照的输出流
     * @return true on success
     */
    boolean onSnapshotLoad(final SnapshotReader reader);

    /**
     * Invoked when the belonging node becomes the leader of the group at |term|
     * Default: Do nothing
     * 代表当前节点 变成了leader  term 代表当前leader 的值
     * @param term new term num
     */
    void onLeaderStart(final long term);

    /**
     * Invoked when this node steps down from the leader of the replication
     * group and |status| describes detailed information
     * 当当前节点不再是 leader 时 触发  status 代表了 改变的详细信息
     * @param status status info
     */
    void onLeaderStop(final Status status);

    /**
     * This method is called when a critical error was encountered, after this
     * point, no any further modification is allowed to applied to this node
     * until the error is fixed and this node restarts.
     * 当处理任务发生异常  当发生错误后 不再接受新的任务直接节点被重启
     * @param e raft error message
     */
    void onError(final RaftException e);

    /**
     * Invoked when a configuration has been committed to the group.
     * 代表当某个配置信息提交到 group 时触发
     * @param conf committed configuration
     */
    void onConfigurationCommitted(final Configuration conf);

    /**
     * This method is called when a follower stops following a leader and its leaderId becomes null,
     * situations including:
     * 1. handle election timeout and start preVote
     * 2. receive requests with higher term such as VoteRequest from a candidate
     *    or appendEntries request from a new leader
     * 3. receive timeoutNow request from current leader and start request vote.
     * 
     * the parameter ctx gives the information(leaderId, term and status) about the
     * very leader whom the follower followed before.
     * User can reset the node's information as it stops following some leader.
     * 停止跟随时触发
     * @param ctx context of leader change
     */
    void onStopFollowing(final LeaderChangeContext ctx);

    /**
     * This method is called when a follower or candidate starts following a leader and its leaderId
     * (should be NULL before the method is called) is set to the leader's id,
     * situations including:
     * 1. a candidate receives appendEntries request from a leader
     * 2. a follower(without leader) receives appendEntries from a leader
     * 
     * the parameter ctx gives the information(leaderId, term and status) about
     * the very leader whom the follower starts to follow.
     * User can reset the node's information as it starts to follow some leader.
     * 代表一个 candidate 或者 follower 跟随一个 leader 时触发
     * @param ctx context of leader change
     */
    void onStartFollowing(final LeaderChangeContext ctx);
}
