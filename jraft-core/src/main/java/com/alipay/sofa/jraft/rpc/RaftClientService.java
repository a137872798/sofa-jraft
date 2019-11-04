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
 * Raft client RPC service.
 * 在基础的client 上增加了 jraft 的 特有命令
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 5:59:52 PM
 */
public interface RaftClientService extends ClientService {

    /**
     * Sends a pre-vote request and handle the response with done.
     * 预投票
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> preVote(Endpoint endpoint, RpcRequests.RequestVoteRequest request,
                            RpcResponseClosure<RpcRequests.RequestVoteResponse> done);

    /**
     * Sends a request-vote request and handle the response with done.
     * 发起投票
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> requestVote(Endpoint endpoint, RpcRequests.RequestVoteRequest request,
                                RpcResponseClosure<RpcRequests.RequestVoteResponse> done);

    /**
     * Sends a append-entries request and handle the response with done.
     * 发送一个 增加数据的请求
     * @param endpoint  destination address (ip, port)  某个follower 对应的地址
     * @param timeoutMs 默认超时时间为-1
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> appendEntries(Endpoint endpoint, RpcRequests.AppendEntriesRequest request, int timeoutMs,
                                  RpcResponseClosure<RpcRequests.AppendEntriesResponse> done);

    /**
     * Sends a install-snapshot request and handle the response with done.
     * 作为leader 时 向其他节点发送快照
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param done      callback
     * @return a future result
     */
    Future<Message> installSnapshot(Endpoint endpoint, RpcRequests.InstallSnapshotRequest request,
                                    RpcResponseClosure<RpcRequests.InstallSnapshotResponse> done);

    /**
     * Get a piece of file data by GetFileRequest, and handle the response with done.
     * 发送一个获取文件的请求
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param timeoutMs timeout millis
     * @param done      callback
     * @return a future result
     */
    Future<Message> getFile(Endpoint endpoint, RpcRequests.GetFileRequest request, int timeoutMs,
                            RpcResponseClosure<RpcRequests.GetFileResponse> done);

    /**
     * Send a timeout-now request and handle the response with done.
     * 发送一个超时的请求
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param timeoutMs timeout millis
     * @param done      callback
     * @return a future result
     */
    Future<Message> timeoutNow(Endpoint endpoint, RpcRequests.TimeoutNowRequest request, int timeoutMs,
                               RpcResponseClosure<RpcRequests.TimeoutNowResponse> done);

    /**
     * Send a read-index request and handle the response with done.
     * 发送一个读取下标的请求
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param timeoutMs timeout millis
     * @param done      callback
     * @return a future result
     */
    Future<Message> readIndex(Endpoint endpoint, RpcRequests.ReadIndexRequest request, int timeoutMs,
                              RpcResponseClosure<RpcRequests.ReadIndexResponse> done);
}
