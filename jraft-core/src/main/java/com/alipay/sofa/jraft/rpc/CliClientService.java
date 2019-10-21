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
package com.alipay.sofa.jraft.rpc;

import java.util.concurrent.Future;

import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;

/**
 * Cli RPC client service.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 11:15:13 AM
 */
public interface CliClientService extends ClientService {

    /**
     * Adds a peer.
     * 为 group 添加一个同级节点
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> addPeer(Endpoint endpoint, CliRequests.AddPeerRequest request,
                            RpcResponseClosure<CliRequests.AddPeerResponse> done);

    /**
     * Removes a peer.
     * 移除 group 中的某个节点
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> removePeer(Endpoint endpoint, CliRequests.RemovePeerRequest request,
                               RpcResponseClosure<CliRequests.RemovePeerResponse> done);

    /**
     * Reset a peer.
     * 重置某个节点
     * @param endpoint  server address  服务器地址
     * @param request   request data   请求对象
     * @param done      callback    处理后的回调对象
     * @return a future with result
     */
    Future<Message> resetPeer(Endpoint endpoint, CliRequests.ResetPeerRequest request,
                              RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Do a snapshot.
     * 快照不都是 服务器 发往客户端吗 为什么这里有个方法???
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> snapshot(Endpoint endpoint, CliRequests.SnapshotRequest request,
                             RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Change peers.
     * 修改某个节点信息
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> changePeers(Endpoint endpoint, CliRequests.ChangePeersRequest request,
                                RpcResponseClosure<CliRequests.ChangePeersResponse> done);

    /**
     * Get the group leader.
     * 获取集群中的leader 信息
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> getLeader(Endpoint endpoint, CliRequests.GetLeaderRequest request,
                              RpcResponseClosure<CliRequests.GetLeaderResponse> done);

    /**
     * Transfer leadership to other peer.
     * 更换leader
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> transferLeader(Endpoint endpoint, CliRequests.TransferLeaderRequest request,
                                   RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Get all peers of the replication group.
     * 获取该组中所有节点
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> getPeers(Endpoint endpoint, CliRequests.GetPeersRequest request,
                             RpcResponseClosure<CliRequests.GetPeersResponse> done);
}
