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

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Node handle requests processor template.
 * node 请求处理器的模板
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 6:03:25 PM 
 * @param <T>
 */
public abstract class NodeRequestProcessor<T extends Message> extends RpcRequestProcessor<T> {

    public NodeRequestProcessor(Executor executor) {
        super(executor);
    }

    protected abstract Message processRequest0(final RaftServerService serviceService, final T request,
                                               final RpcRequestClosure done);

    protected abstract String getPeerId(final T request);

    protected abstract String getGroupId(final T request);

    /**
     * 处理请求
     * @param request
     * @param done 该对象内部包含了 bizContext(包含通讯2端的信息, asyncContext具备将res返回给client的能力)
     * @return
     */
    @Override
    public Message processRequest(T request, RpcRequestClosure done) {
        final PeerId peer = new PeerId();
        // 请求中的peerId 就是client的target  也就是处理该请求的节点地址
        final String peerIdStr = getPeerId(request);
        if (peer.parse(peerIdStr)) {
            final String groupId = getGroupId(request);
            // 尝试从NodeManager中搜索node信息
            final Node node = NodeManager.getInstance().get(groupId, peer);
            // 这个node 是什么时候插入进去的???
            if (node != null) {
                return processRequest0((RaftServerService) node, request, done);
            } else {
                return RpcResponseFactory.newResponse(RaftError.ENOENT, "Peer id not found: %s, group: %s", peerIdStr,
                    groupId);
            }
        } else {
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peerId: %s", peerIdStr);
        }
    }
}
