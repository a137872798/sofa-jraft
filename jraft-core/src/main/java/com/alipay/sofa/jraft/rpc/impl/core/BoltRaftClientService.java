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
package com.alipay.sofa.jraft.rpc.impl.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.impl.AbstractBoltClientService;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.DefaultFixedThreadsExecutorGroupFactory;
import com.alipay.sofa.jraft.util.concurrent.FixedThreadsExecutorGroup;
import com.google.protobuf.Message;

/**
 * Raft rpc service based bolt.
 * 就是一个 client 基本类
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 6:07:05 PM
 */
public class BoltRaftClientService extends AbstractBoltClientService implements RaftClientService {

    // 注意 只有appendEntry相关的请求才从线程组中选择指定的线程 而其他方法调用则是共用某个线程池

    /**
     * 创建线程组对象  默认使用mpsc 作为任务队列 同时该对象 使用了 类似netty 的线程组模式 内部包含一个选择器对象
     * 会负载请求 使用其中一条线程设置任务   阻塞队列本身为什么性能差 是因为不同线程间并发访问下标 必须要加锁
     * 而mpsc 就是专门为这种情况创建的一种无锁队列
     */
    private static final FixedThreadsExecutorGroup  APPEND_ENTRIES_EXECUTORS = DefaultFixedThreadsExecutorGroupFactory.INSTANCE
                                                                                 .newExecutorGroup(
                                                                                     Utils.APPEND_ENTRIES_THREADS_SEND,
                                                                                     "Append-Entries-Thread-Send",
                                                                                     Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD,
                                                                                     true);

    /**
     * 不同的 端点使用不同的 executor 去处理
     */
    private final ConcurrentMap<Endpoint, Executor> appendEntriesExecutorMap = new ConcurrentHashMap<>();

    // cached node options
    private NodeOptions                             nodeOptions;
    /**
     * 抽象为 通过组的 jraft 节点  拥有leader 更换 获取每个peer信息等api
     */
    private final ReplicatorGroup                   rgGroup;

    /**
     * 配置客户端
     * @param rpcClient
     */
    @Override
    protected void configRpcClient(final RpcClient rpcClient) {
        // 设置一个连接处理器
        rpcClient.addConnectionEventProcessor(ConnectionEventType.CONNECT, new ClientServiceConnectionEventProcessor(
            this.rgGroup));
    }

    public BoltRaftClientService(final ReplicatorGroup rgGroup) {
        this.rgGroup = rgGroup;
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        final boolean ret = super.init(rpcOptions);
        if (ret) {
            this.nodeOptions = (NodeOptions) rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> preVote(final Endpoint endpoint, final RequestVoteRequest request,
                                   final RpcResponseClosure<RequestVoteResponse> done) {
        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    /**
     * 某个候选人尝试访问某个follower 并拉票
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param done      callback
     * @return
     */
    @Override
    public Future<Message> requestVote(final Endpoint endpoint, final RequestVoteRequest request,
                                       final RpcResponseClosure<RequestVoteResponse> done) {
        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    /**
     * 代表该client 对象将某个请求发送到endpoint 对应的 follower上
     * @param endpoint  destination address (ip, port)  某个follower 对应的地址
     * @param request   request data
     * @param timeoutMs 默认超时时间为-1
     * @param done      callback
     * @return
     */
    @Override
    public Future<Message> appendEntries(final Endpoint endpoint, final AppendEntriesRequest request,
                                         final int timeoutMs, final RpcResponseClosure<AppendEntriesResponse> done) {
        // 通过选择器 相对公平的选择某条线程作为始终处理该endpoint 相关发送逻辑的线程  因为leader 向每个follower 发送的数据可以是完全相同的
        // 所以使用 rr 可以保证公平性  实际上任何线程池 都可以考虑使用mpsc 来提高性能
        // 为什么要使用 线程组 因为默认的线程池实现 当其他线程处理完任务后都会从阻塞队列中拉取任务 那么实际上不符合 mpsc的模型
        // 而是 mpmc 所以 mpsc作为阻塞队列+线程组+选择器  是一种固定搭配  直接往JUC 实现的线程池中替换mpsc是没用的
        final Executor executor = this.appendEntriesExecutorMap.computeIfAbsent(endpoint, k -> APPEND_ENTRIES_EXECUTORS.next());
        // 使用指定的线程池执行任务
        return invokeWithDone(endpoint, request, done, timeoutMs, executor);
    }

    @Override
    public Future<Message> getFile(final Endpoint endpoint, final GetFileRequest request, final int timeoutMs,
                                   final RpcResponseClosure<GetFileResponse> done) {
        // open checksum
        final InvokeContext ctx = new InvokeContext();
        ctx.put(InvokeContext.BOLT_CRC_SWITCH, true);
        return invokeWithDone(endpoint, request, ctx, done, timeoutMs);
    }

    @Override
    public Future<Message> installSnapshot(final Endpoint endpoint, final InstallSnapshotRequest request,
                                           final RpcResponseClosure<InstallSnapshotResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcInstallSnapshotTimeout());
    }

    @Override
    public Future<Message> timeoutNow(final Endpoint endpoint, final TimeoutNowRequest request, final int timeoutMs,
                                      final RpcResponseClosure<TimeoutNowResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> readIndex(final Endpoint endpoint, final ReadIndexRequest request, final int timeoutMs,
                                     final RpcResponseClosure<ReadIndexResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }
}
