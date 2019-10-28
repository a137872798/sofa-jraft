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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.util.Describer;

/**
 * Executing Snapshot related stuff.
 * 生成自身的快照
 * Describer 代表该对象具备生成自己的描述信息
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 2:27:02 PM
 */
public interface SnapshotExecutor extends Lifecycle<SnapshotExecutorOptions>, Describer {

    /**
     * Return the owner NodeImpl
     * 获取本节点
     */
    NodeImpl getNode();

    /**
     * Start to snapshot StateMachine, and |done| is called after the
     * execution finishes or fails.
     * 开始触发快照相关的状态机api
     * @param done snapshot callback
     */
    void doSnapshot(final Closure done);

    /**
     * Install snapshot according to the very RPC from leader
     * After the installing succeeds (StateMachine is reset with the snapshot)
     * or fails, done will be called to respond
     * Errors:
     *  - Term mismatches: which happens interrupt_downloading_snapshot was 
     *    called before install_snapshot, indicating that this RPC was issued by
     *    the old leader.
     *  - Interrupted: happens when interrupt_downloading_snapshot is called or
     *    a new RPC with the same or newer snapshot arrives
     * - Busy: the state machine is saving or loading snapshot
     * 将快照设置到其他节点
     */
    void installSnapshot(final InstallSnapshotRequest request, final InstallSnapshotResponse.Builder response,
                         final RpcRequestClosure done);

    /**
     * Interrupt the downloading if possible.
     * This is called when the term of node increased to |new_term|, which
     * happens when receiving RPC from new peer. In this case, it's hard to
     * determine whether to keep downloading snapshot as the new leader
     * possibly contains the missing logs and is going to send AppendEntries. To
     * make things simplicity and leader changing during snapshot installing is 
     * very rare. So we interrupt snapshot downloading when leader changes, and
     * let the new leader decide whether to install a new snapshot or continue 
     * appending log entries.
     * 
     * NOTE: we can't interrupt the snapshot installing which has finished
     *  downloading and is reseting the State Machine.
     *  当leader 改变时 停止从其他节点下载快照
     *
     * @param newTerm new term num
     */
    void interruptDownloadingSnapshots(final long newTerm);

    /**
     * Returns true if this is currently installing a snapshot, either
     * downloading or loading.
     * 是否正在安装快照
     */
    boolean isInstallingSnapshot();

    /**
     * Returns the backing snapshot storage
     * 获取保存快照的存储对象
     */
    SnapshotStorage getSnapshotStorage();

    /**
     * Block the current thread until all the running job finishes (including failure)
     * 阻塞当前线程 直到所有任务结束
     */
    void join() throws InterruptedException;
}
