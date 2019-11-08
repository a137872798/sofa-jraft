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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Process get leader request.
 * 处理从某个节点上获取leader 的请求 可能该节点不是leader 也可能  该节点连接到的leader 已经失效 比如脑裂
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 2:43:20 PM
 */
public class GetLeaderRequestProcessor extends BaseCliRequestProcessor<GetLeaderRequest> {

    public GetLeaderRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * @param request
     * @return
     */
    @Override
    protected String getPeerId(GetLeaderRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(GetLeaderRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(CliRequestContext ctx, GetLeaderRequest request, RpcRequestClosure done) {
        //ignore
        return null;
    }

    /**
     * 原本 父类方法会通过peerId 和 groupId 获取 node 信息 (从NodeManager 中)
     * 这里是有可能获取到无效的leader的 在某个leader 无法连接到其他节点时 而follower 检测心跳还没有超时 （follower检测心跳超时的时间是leader自我检测的2倍）
     * @param request
     * @param done
     * @return
     */
    @Override
    public Message processRequest(GetLeaderRequest request, RpcRequestClosure done) {
        List<Node> nodes = new ArrayList<>();
        // 获取请求体中的目标group
        String groupId = getGroupId(request);

        if (request.hasPeerId()) {
            String peerIdStr = getPeerId(request);
            PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                Status st = new Status();
                // 找到节点对象 并设置到list 中
                nodes.add(getNode(groupId, peer, st));
                if (!st.isOk()) {
                    return RpcResponseFactory.newResponse(st);
                }
            } else {
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer id %", peerIdStr);
            }
        } else {
            // 没有设置peerId 时走这条分支 这个manager 应该是以进程为单位的 这里是查询 某台机器上创建了多少node 并且是同组的  最少也会有一个
            nodes = NodeManager.getInstance().getNodesByGroupId(groupId);
        }
        // 代表server 还没有启动
        if (nodes == null || nodes.isEmpty()) {
            return RpcResponseFactory.newResponse(RaftError.ENOENT, "No nodes in group %s", groupId);
        }
        for (Node node : nodes) {
            // 因为每个节点中都包含了 leaderId 这里可以直接获取
            PeerId leader = node.getLeaderId();
            if (leader != null && !leader.isEmpty()) {
                return GetLeaderResponse.newBuilder().setLeaderId(leader.toString()).build();
            }
        }
        return RpcResponseFactory.newResponse(RaftError.EAGAIN, "Unknown leader");
    }

    @Override
    public String interest() {
        return GetLeaderRequest.class.getName();
    }

}
