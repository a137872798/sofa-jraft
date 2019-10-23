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
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.google.protobuf.Message;

import static com.alipay.sofa.jraft.rpc.CliRequests.GetPeersRequest;
import static com.alipay.sofa.jraft.rpc.CliRequests.GetPeersResponse;

/**
 * Process get all peers of the replication group request.
 * 找到某个组中所有的节点
 * @author jiachun.fjc
 */
public class GetPeersRequestProcessor extends BaseCliRequestProcessor<GetPeersRequest> {

    public GetPeersRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * 这里返回leader 是因为只有leader 才能正常调用  listAlivePeers listPeers
     * @param request
     * @return
     */
    @Override
    protected String getPeerId(GetPeersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(GetPeersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(CliRequestContext ctx, GetPeersRequest request, RpcRequestClosure done) {
        final List<PeerId> peers;
        // 判断是否只获取存活节点
        if (request.hasOnlyAlive() && request.getOnlyAlive()) {
            peers = ctx.node.listAlivePeers();
        } else {
            peers = ctx.node.listPeers();
        }
        final GetPeersResponse.Builder builder = GetPeersResponse.newBuilder();
        for (final PeerId peerId : peers) {
            builder.addPeers(peerId.toString());
        }
        // 这里构建消息 通过父类进行发送 而不是通过回调
        return builder.build();
    }

    @Override
    public String interest() {
        return GetPeersRequest.class.getName();
    }
}
