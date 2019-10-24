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
package com.alipay.sofa.jraft.rpc.impl.cli;

import java.util.List;
import java.util.concurrent.Executor;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Base template to handle cli requests.
 * @author boyan (boyan@alibaba-inc.com)
 * 请求处理器基类   RpcRequestProcessor 读取数据并调用processRequest
 * 2018-Apr-09 11:51:42 AM 
 * @param <T>
 */
public abstract class BaseCliRequestProcessor<T extends Message> extends RpcRequestProcessor<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(BaseCliRequestProcessor.class);

    public BaseCliRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * Returns the peerId that will be find in node manager.
     * 获取 节点ID
     */
    protected abstract String getPeerId(T request);

    /**
     * Returns the raft group id
     * 获取 组ID
     */
    protected abstract String getGroupId(T request);

    /**
     * Process the request with CliRequestContext
     * 处理请求 并触发 回调
     */
    protected abstract Message processRequest0(CliRequestContext ctx, T request, RpcRequestClosure done);

    /**
     * Cli request context
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-09 11:55:45 AM
     */
    public static class CliRequestContext {

        /**
         * The found node.
         */
        public final Node   node;
        /**
         * The peerId in returns by {@link BaseCliRequestProcessor#getPeerId(Message)}, null if absent.
         */
        public final PeerId peerId;
        /**
         * The groupId in request.
         */
        public final String groupId;

        public CliRequestContext(Node ndoe, String groupId, PeerId peerId) {
            super();
            this.node = ndoe;
            this.peerId = peerId;
            this.groupId = groupId;
        }

    }

    /**
     * 父类接受到请求后会触发该方法
     * @param request
     * @param done  回调方法
     * @return
     */
    @Override
    public Message processRequest(T request, RpcRequestClosure done) {

        // 获取当前节点所在的 组 和节点id
        String groupId = getGroupId(request);
        String peerIdStr = getPeerId(request);
        PeerId peerId = null;

        if (!StringUtils.isBlank(peerIdStr)) {
            peerId = new PeerId();
            if (!peerId.parse(peerIdStr)) {
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer: %s", peerIdStr);
            }
        }

        Status st = new Status();
        // 寻找特殊的 node
        Node node = getNode(groupId, peerId, st);
        if (!st.isOk()) {
            return RpcResponseFactory.newResponse(st.getCode(), st.getErrorMsg());
        } else {
            // 委托下层实现
            return processRequest0(new CliRequestContext(node, groupId, peerId), request, done);
        }
    }

    /**
     * 根据 组id 节点id 和状态 获取节点对象
     * @param groupId
     * @param peerId
     * @param st
     * @return
     */
    protected Node getNode(String groupId, PeerId peerId, Status st) {
        Node node = null;

        if (peerId != null) {
            // 从 NodeManager 中找到节点并返回
            node = NodeManager.getInstance().get(groupId, peerId);
            if (node == null) {
                st.setError(RaftError.ENOENT, "Fail to find node %s in group %s", peerId, groupId);
            }
        } else {
            // 获得某个组下所有节点
            List<Node> nodes = NodeManager.getInstance().getNodesByGroupId(groupId);
            if (nodes == null || nodes.isEmpty()) {
                st.setError(RaftError.ENOENT, "Empty nodes in group %s", groupId);
                // 不允许超过1个???
            } else if (nodes.size() > 1) {
                st.setError(RaftError.EINVAL, "Peer must be specified since there're %d nodes in group %s",
                    nodes.size(), groupId);
            } else {
                node = nodes.get(0);
            }

        }
        // 该节点不支持 命令行 处理
        if (node != null && node.getOptions().isDisableCli()) {
            st.setError(RaftError.EACCES, "Cli service is not allowed to access node %s", node.getNodeId());
        }
        return node;
    }
}
