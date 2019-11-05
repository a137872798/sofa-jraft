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

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.commons.lang.StringUtils;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.Connection;
import com.alipay.remoting.ConnectionEventProcessor;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequestHeader;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.MpscSingleThreadExecutor;
import com.alipay.sofa.jraft.util.concurrent.SingleThreadExecutor;
import com.google.protobuf.Message;

/**
 * Append entries request processor.
 * 该对象作为 node 节点的server处理器 用于处理从 client 接受到的请求  该处理器是在什么时机设置的???
 * 实现了连接事件处理器
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 3:00:13 PM
 */
public class AppendEntriesRequestProcessor extends NodeRequestProcessor<AppendEntriesRequest> implements
                                                                                             ConnectionEventProcessor {

    static final String PEER_ATTR = "jraft-peer";

    /**
     * Peer executor selector.
     * @author dennis
     */
    final class PeerExecutorSelector implements ExecutorSelector {

        PeerExecutorSelector() {
            super();
        }

        /**
         * 貌似是根据事件来选择处理器的
         * @param requestClass
         * @param requestHeader
         * @return
         */
        @Override
        public Executor select(final String requestClass, final Object requestHeader) {
            final AppendEntriesRequestHeader header = (AppendEntriesRequestHeader) requestHeader;
            final String groupId = header.getGroupId();
            final String peerId = header.getPeerId();

            final PeerId peer = new PeerId();

            // 解析失败 触发 获取 执行器
            if (!peer.parse(peerId)) {
                return getExecutor();
            }

            final Node node = NodeManager.getInstance().get(groupId, peer);

            // 节点不存在 或者不在管道中 也是返回执行器
            if (node == null || !node.getRaftOptions().isReplicatorPipeline()) {
                return getExecutor();
            }

            // The node enable pipeline, we should ensure bolt support it.
            // 判断能使用管道
            Utils.ensureBoltPipeline();

            // 使用请求上下文中的 执行器 特意抽象出一个选择器 来选择执行器 这里是什么意思呢 ??? //

            // 从缓存中获取上下文
            final PeerRequestContext ctx = getPeerRequestContext(groupId, peerId, null);

            // 返回上下文的线程池  该线程池 是一个 多生产者单消费者队列  单线程线程池
            return ctx.executor;
        }
    }

    /**
     * RpcRequestClosure that will send responses in pipeline mode.
     * 按照顺序来返回res  实际上是为了避免乱序问题
     * @author dennis
     */
    class SequenceRpcRequestClosure extends RpcRequestClosure {

        /**
         * 当前处理的序列
         */
        private final int    reqSequence;
        /**
         * 本node 所在组的id
         */
        private final String groupId;
        /**
         * 本node 的id
         */
        private final String peerId;

        public SequenceRpcRequestClosure(RpcRequestClosure parent, int sequence, String groupId, String peerId) {
            super(parent.getBizContext(), parent.getAsyncContext());
            this.reqSequence = sequence;
            this.groupId = groupId;
            this.peerId = peerId;
        }

        /**
         * 通过管道发送结果
         * @param msg
         */
        @Override
        public void sendResponse(final Message msg) {
            sendSequenceResponse(this.groupId, this.peerId, this.reqSequence, getAsyncContext(), getBizContext(), msg);
        }
    }

    /**
     * Response message wrapper with a request sequence number and asyncContext.done
     * 在管道中处理的消息
     * @author dennis
     */
    static class SequenceMessage implements Comparable<SequenceMessage> {
        /**
         * 消息
         */
        public final Message       msg;
        private final int          sequence;
        /**
         * 异步上下文
         */
        private final AsyncContext asyncContext;

        public SequenceMessage(AsyncContext asyncContext, Message msg, int sequence) {
            super();
            this.asyncContext = asyncContext;
            this.msg = msg;
            this.sequence = sequence;
        }

        /**
         * Send the response.
         * 通过异步上下文发送结果
         */
        void sendResponse() {
            this.asyncContext.sendResponse(this.msg);
        }

        /**
         * Order by sequence number
         * 重写该方法用于 优先队列中排序
         */
        @Override
        public int compareTo(final SequenceMessage o) {
            return Integer.compare(this.sequence, o.sequence);
        }
    }

    /**
     * Send request in pipeline mode.
     * 按照管道模式 发送响应结果 因为网络关系 可能用户端 后写入的数据反而会先被follower 收到 这里利用优先队列做了 排序 确保不会发生乱序
     * 不过该序列值是由 处理消息的那刻确定的 实际上是一个先进先出的队列 如果一开始收到的消息就是乱序的呢
     */
    void sendSequenceResponse(final String groupId, final String peerId, final int seq,
                              final AsyncContext asyncContext, final BizContext bizContext, final Message msg) {
        // 获取连接对象  内部包含了netty 的 Channel 对象 可以发送请求
        final Connection connection = bizContext.getConnection();
        // 获取上下文  上下文本身在全局范围内会被复用
        final PeerRequestContext ctx = getPeerRequestContext(groupId, peerId, connection);
        // 获取优先队列  也就是消息并没有直接发送 而是先存放在一个优先队列中
        final PriorityQueue<SequenceMessage> respQueue = ctx.responseQueue;
        assert (respQueue != null);

        // 通过内置锁 确保优先队列在被触发回调的多个线程中 线程安全
        synchronized (Utils.withLockObject(respQueue)) {
            // 将msg 对象封装成了序列对象后添加到优先队列中 通过序列号来确定优先级
            respQueue.add(new SequenceMessage(asyncContext, msg, seq));

            if (!ctx.hasTooManyPendingResponses()) {
                // 每当成功触发一次回调 需要发送一次res 到 client时
                // 因为回调触发的时机是不确定的 比如写入LogEntry 是在触发flush 时 才会触发回调 相对会有延时
                while (!respQueue.isEmpty()) {
                    final SequenceMessage queuedPipelinedResponse = respQueue.peek();

                    // 确保优先返回 最早序列的 res
                    if (queuedPipelinedResponse.sequence != getNextRequiredSequence(groupId, peerId, connection)) {
                        // sequence mismatch, waiting for next response.
                        break;
                    }
                    // 该 方法就是移除优先队列最上方的对象
                    respQueue.remove();
                    try {
                        // 该对象内部包含了 发送res的asyncContext
                        queuedPipelinedResponse.sendResponse();
                    } finally {
                        // 增加序列值
                        getAndIncrementNextRequiredSequence(groupId, peerId, connection);
                    }
                }
            // 等待响应的数据过多时如何处理
            } else {
                LOG.warn("Closed connection to peer {}/{}, because of too many pending responses, queued={}, max={}",
                    ctx.groupId, peerId, respQueue.size(), ctx.maxPendingResponses);
                // 关闭channel 这样会有异常信息返回给client吗 看来这段req 都会丢弃了 然后下次传入的req 又会建立新的requestContext
                connection.close();
                // Close the connection if there are too many pending responses in queue.
                // 销毁对应的上下文对象
                removePeerRequestContext(groupId, peerId);
            }
        }
    }

    /**
     * 节点请求上下文
     */
    static class PeerRequestContext {

        // 该节点信息
        private final String                         groupId;
        private final String                         peerId;

        // Executor to run the requests
        // 单线程处理器
        private SingleThreadExecutor                 executor;
        // The request sequence;
        // 当前收到的请求对应的序列
        private int                                  sequence;
        // The required sequence to be sent.
        // 当前预计返回的 res 对应的序列
        private int                                  nextRequiredSequence;
        // The response queue,it's not thread-safe and protected by it self object monitor.
        // 存放响应结果的 优先队列
        private final PriorityQueue<SequenceMessage> responseQueue;

        /**
         * 最大悬置结果数  每个预备返回的响应结果会存放在优先队列中之后尽可能统一返回
         */
        private final int                            maxPendingResponses;

        /**
         * peer 的请求上下文
         * @param groupId
         * @param peerId
         * @param maxPendingResponses  允许最大悬置的 响应结果数量
         */
        public PeerRequestContext(final String groupId, final String peerId, final int maxPendingResponses) {
            super();
            this.peerId = peerId;
            this.groupId = groupId;
            // 多生产 单消费 线程池  这里限定了 队列大小 超过该数量 任务会被拒绝
            this.executor = new MpscSingleThreadExecutor(Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD,
                JRaftUtils.createThreadFactory(groupId + "/" + peerId + "-AppendEntriesThread"));

            this.sequence = 0;
            this.nextRequiredSequence = 0;
            this.maxPendingResponses = maxPendingResponses;
            this.responseQueue = new PriorityQueue<>(50);
        }

        boolean hasTooManyPendingResponses() {
            return this.responseQueue.size() > this.maxPendingResponses;
        }

        /**
         * 增加该上下文已经处理的请求数量
         * @return
         */
        int getAndIncrementSequence() {
            final int prev = this.sequence;
            this.sequence++;
            if (this.sequence < 0) {
                this.sequence = 0;
            }
            return prev;
        }

        synchronized void destroy() {
            if (this.executor != null) {
                LOG.info("Destroyed peer request context for {}/{}", this.groupId, this.peerId);
                this.executor.shutdownGracefully();
                this.executor = null;
            }
        }

        /**
         * 返回下一个应该处理的序列
         * @return
         */
        int getNextRequiredSequence() {
            return this.nextRequiredSequence;
        }

        int getAndIncrementNextRequiredSequence() {
            final int prev = this.nextRequiredSequence;
            this.nextRequiredSequence++;
            if (this.nextRequiredSequence < 0) {
                this.nextRequiredSequence = 0;
            }
            return prev;
        }
    }

    /**
     * 根据 groupId 和 peerId 来获取请求上下文
     * @param groupId
     * @param peerId
     * @param conn
     * @return
     */
    PeerRequestContext getPeerRequestContext(final String groupId, final String peerId, final Connection conn) {
        // 从一个 map 中获取上下文信息
        ConcurrentMap<String/* peerId */, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);
        if (groupContexts == null) {
            groupContexts = new ConcurrentHashMap<>();
            // 模板代码 用于往 ConcurrentMap 中插入数据
            final ConcurrentMap<String, PeerRequestContext> existsCtxs = this.peerRequestContexts.putIfAbsent(groupId,
                groupContexts);
            if (existsCtxs != null) {
                groupContexts = existsCtxs;
            }
        }

        PeerRequestContext peerCtx = groupContexts.get(peerId);
        if (peerCtx == null) {
            // 生成上下文对象 并存入map 中
            // Utils.withLockObject(groupContexts) 就是非空校验
            synchronized (Utils.withLockObject(groupContexts)) {
                peerCtx = groupContexts.get(peerId);
                // double check in lock
                if (peerCtx == null) {
                    // only one thread to process append entries for every jraft node
                    final PeerId peer = new PeerId();
                    final boolean parsed = peer.parse(peerId);
                    assert (parsed);
                    final Node node = NodeManager.getInstance().get(groupId, peer);
                    assert (node != null);
                    // 这里保存了 maxInflight 代表 服务端也有做处理数量的限制
                    peerCtx = new PeerRequestContext(groupId, peerId, node.getRaftOptions()
                        .getMaxReplicatorInflightMsgs());
                    groupContexts.put(peerId, peerCtx);
                }
            }
        }
        // Set peer attribute into connection if absent
        // 这里为连接设置了 peerId 的属性 该id 就对应本server的节点id
        if (conn != null && conn.getAttribute(PEER_ATTR) == null) {
            conn.setAttribute(PEER_ATTR, peerId);
        }
        return peerCtx;
    }

    void removePeerRequestContext(final String groupId, final String peerId) {
        final ConcurrentMap<String/* peerId */, PeerRequestContext> groupContexts = this.peerRequestContexts
            .get(groupId);
        if (groupContexts == null) {
            return;
        }
        synchronized (Utils.withLockObject(groupContexts)) {
            final PeerRequestContext ctx = groupContexts.remove(peerId);
            if (ctx != null) {
                ctx.destroy();
            }
        }
    }

    /**
     * RAFT group peer request contexts
     * Map<groupId, <peerId, ctx>>
     *     上下文缓存  key1 是组id  key2 是 节点id value 是上下文
     */
    private final ConcurrentMap<String, ConcurrentMap<String, PeerRequestContext>> peerRequestContexts = new ConcurrentHashMap<>();

    /**
     * The executor selector to select executor for processing request.
     * 执行选择器
     */
    private final ExecutorSelector                                                 executorSelector;

    /**
     * 初始化该对象时 会创建一个执行选择器
     * @param executor
     */
    public AppendEntriesRequestProcessor(Executor executor) {
        super(executor);
        // 请求对象满足条件时 会返回上下文中的单线程线程池 否则返回 executor
        this.executorSelector = new PeerExecutorSelector();
    }

    @Override
    protected String getPeerId(final AppendEntriesRequest request) {
        // 就是返回client 的地址
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final AppendEntriesRequest request) {
        return request.getGroupId();
    }

    private int getAndIncrementSequence(final String groupId, final String peerId, final Connection conn) {
        return getPeerRequestContext(groupId, peerId, conn).getAndIncrementSequence();
    }

    /**
     * 获取下一个 期望返回的res对应的序列
     * @param groupId
     * @param peerId
     * @param conn
     * @return
     */
    private int getNextRequiredSequence(final String groupId, final String peerId, final Connection conn) {
        return getPeerRequestContext(groupId, peerId, conn).getNextRequiredSequence();
    }

    private int getAndIncrementNextRequiredSequence(final String groupId, final String peerId, final Connection conn) {
        return getPeerRequestContext(groupId, peerId, conn).getAndIncrementNextRequiredSequence();
    }

    /**
     * 处理请求最后会转发到该方法
     * @param service  本server   实际上是一个node对象
     * @param request
     * @param done
     * @return
     */
    @Override
    public Message processRequest0(final RaftServerService service, final AppendEntriesRequest request,
                                   final RpcRequestClosure done) {

        // 因为传入的是node 对象所以可以这样转换
        final Node node = (Node) service;

        // 是否使用管道  这样的话 res返回的顺序就跟 req 传入的顺序一致了  非管道模式就不需要 sequence 和 requireSequence
        if (node.getRaftOptions().isReplicatorPipeline()) {
            // groupId 和 peerId 实际上就是指向该server
            final String groupId = request.getGroupId();
            final String peerId = request.getPeerId();

            // 如果某次悬置的 res 过多requestContext 会被删除 (同时 connection 也会被关闭)这里又会重新创建 那么 那些被丢弃的 req 会怎么样呢??? 会有res 通知到client吗???
            // 每个请求应该是有个时限的超过了自动返回超时异常
            final int reqSequence = getAndIncrementSequence(groupId, peerId, done.getBizContext().getConnection());
            // 使用node 处理添加LogEntry的任务 这里将 回调又包装了一层
            final Message response = service.handleAppendEntriesRequest(request, new SequenceRpcRequestClosure(done,
                reqSequence, groupId, peerId));
            // 如果返回了 res 代表中途发生了异常情况 且不会被回调处理  需要手动发送结果
            if (response != null) {
                sendSequenceResponse(groupId, peerId, reqSequence, done.getAsyncContext(), done.getBizContext(),
                    response);
            }
            // 这里同样返回null 避免上层继续发送
            return null;
        } else {
            // 非管道模式 这里就是不将结果排序
            return service.handleAppendEntriesRequest(request, done);
        }
    }

    @Override
    public String interest() {
        return AppendEntriesRequest.class.getName();
    }

    @Override
    public ExecutorSelector getExecutorSelector() {
        return this.executorSelector;
    }

    // TODO called when shutdown service.
    public void destroy() {
        for (final ConcurrentMap<String/* peerId */, PeerRequestContext> map : this.peerRequestContexts.values()) {
            for (final PeerRequestContext ctx : map.values()) {
                ctx.destroy();
            }
        }
    }

    /**
     * 当连接到 server 时 会触发该方法
     * @param remoteAddr
     * @param conn
     */
    @Override
    public void onEvent(final String remoteAddr, final Connection conn) {
        final PeerId peer = new PeerId();
        final String peerAttr = (String) conn.getAttribute(PEER_ATTR);

        // 如果存在节点信息
        if (!StringUtils.isBlank(peerAttr) && peer.parse(peerAttr)) {
            // Clear request context when connection disconnected.
            for (final Map.Entry<String, ConcurrentMap<String, PeerRequestContext>> entry : this.peerRequestContexts
                .entrySet()) {
                final ConcurrentMap<String, PeerRequestContext> groupCtxs = entry.getValue();
                synchronized (Utils.withLockObject(groupCtxs)) {
                    // 找到对应上下文并移除
                    final PeerRequestContext ctx = groupCtxs.remove(peer.toString());
                    if (ctx != null) {
                        ctx.destroy();
                    }
                }
            }
        } else {
            LOG.info("Connection disconnected: {}", remoteAddr);
        }
    }
}
