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

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetPeerRequest;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Reset peer request processor.
 * 重置节点的处理器
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 2:38:32 PM
 */
public class ResetPeerRequestProcessor extends BaseCliRequestProcessor<ResetPeerRequest> {

    public ResetPeerRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * 该请求不是针对 leader 了
     * @param request
     * @return
     */
    @Override
    protected String getPeerId(ResetPeerRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(ResetPeerRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(CliRequestContext ctx, ResetPeerRequest request, RpcRequestClosure done) {
        Configuration newConf = new Configuration();
        for (String peerIdStr : request.getNewPeersList()) {
            PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                newConf.addPeer(peer);
            } else {
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        LOG.info("Receive ResetPeerRequest to {} from {}, new conf is {}", ctx.node.getNodeId(), done.getBizContext()
            .getRemoteAddress(), newConf);
        // 为 req中包含的node 对象重置peer
        Status st = ctx.node.resetPeers(newConf);
        return RpcResponseFactory.newResponse(st);
    }

    @Override
    public String interest() {
        return ResetPeerRequest.class.getName();
    }

}
