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

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * AddPeer request processor.
 * 为整个group 增加一个节点  只有leader 能处理该请求 在 CliServiceImpl 中 发往某个group中任意节点尝试增加节点 都会先从group中获取leaderId 之后将请求发往leader
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 11:33:50 AM
 */
public class AddPeerRequestProcessor extends BaseCliRequestProcessor<AddPeerRequest> {

    public AddPeerRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * 父类 查询Node时 会调用该 方法 也就是获取 addPeer的 leader节点 之后将请求的节点对象添加到节点组中
     * 是否每个 client 上都有 nodeManager 呢 该对象内部的信息又是由谁来同步的 (多个注册中心之间)
     * @param request
     * @return
     */
    @Override
    protected String getPeerId(AddPeerRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(AddPeerRequest request) {
        return request.getGroupId();
    }

    /**
     * 处理请求
     * @param ctx  上下文信息  包含 groupId PeerId  Node
     * @param request
     * @param done
     * @return
     */
    @Override
    protected Message processRequest0(CliRequestContext ctx, AddPeerRequest request, RpcRequestClosure done) {
        // 获取该节点的同级节点  这里是深拷贝
        List<PeerId> oldPeers = ctx.node.listPeers();
        // 获取要新增的节点
        String addingPeerIdStr = request.getPeerId();
        PeerId addingPeer = new PeerId();
        if (addingPeer.parse(addingPeerIdStr)) {
            LOG.info("Receive AddPeerRequest to {} from {}, adding {}", ctx.node.getNodeId(), done.getBizContext()
                .getRemoteAddress(), addingPeerIdStr);
            // 为node 节点增加同级节点
            ctx.node.addPeer(addingPeer,
                    // 回调对象  失败的话 直接调用本次处理请求的回调对象
                    status -> {
                if (!status.isOk()) {
                    done.run(status);
                } else {
                    AddPeerResponse.Builder rb = AddPeerResponse.newBuilder();
                    boolean alreadyExists = false;
                    for (PeerId oldPeer : oldPeers) {
                        rb.addOldPeers(oldPeer.toString());
                        rb.addNewPeers(oldPeer.toString());
                        if (oldPeer.equals(addingPeer)) {
                            alreadyExists = true;
                        }
                    }
                    if (!alreadyExists) {
                        rb.addNewPeers(addingPeerIdStr);
                    }
                    // 将当前集群快照返回给 请求方
                    done.sendResponse(rb.build());
                }
            });
        // 无法解析 提示错误
        } else {
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer id %", addingPeerIdStr);
        }

        //  正常情况返回 null 如果返回了一个消息体 那么 在父类会发送该消息
        return null;
    }

    @Override
    public String interest() {
        return AddPeerRequest.class.getName();
    }

}
