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

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.ChangePeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.ChangePeersResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Change peers request processor.
 * 修改节点处理器  (req 中包含一组新的node 这里是更替整个 group 的节点)
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 3:09:23 PM
 */
public class ChangePeersRequestProcessor extends BaseCliRequestProcessor<ChangePeersRequest> {

    public ChangePeersRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * 注意 peerId 是 leaderId
     * @param request
     * @return
     */
    @Override
    protected String getPeerId(ChangePeersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(ChangePeersRequest request) {
        return request.getGroupId();
    }

    /**
     * 当发送 ChangePeers请求时  旧的集群点就是 oldConf 请求中包含的新节点信息 就是 conf
     * @param ctx
     * @param request
     * @param done
     * @return
     */
    @Override
    protected Message processRequest0(CliRequestContext ctx, ChangePeersRequest request, RpcRequestClosure done) {
        // 获取当前组中所有节点
        List<PeerId> oldConf = ctx.node.listPeers();

        Configuration conf = new Configuration();
        // 请求中包含一组新的节点
        for (String peerIdStr : request.getNewPeersList()) {
            PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                conf.addPeer(peer);
            } else {
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        LOG.info("Receive ChangePeersRequest to {} from {}, new conf is {}", ctx.node.getNodeId(), done.getBizContext()
            .getRemoteAddress(), conf);
        // 更换节点 并触发回调
        ctx.node.changePeers(conf, status -> {
            if (!status.isOk()) {
                done.run(status);
            } else {
                // 通过 RpcRequestClosure 将结果返回
                ChangePeersResponse.Builder rb = ChangePeersResponse.newBuilder();
                for (PeerId peer : oldConf) {
                    rb.addOldPeers(peer.toString());
                }
                for (PeerId peer : conf) {
                    rb.addNewPeers(peer.toString());
                }
                done.sendResponse(rb.build());
            }
        });
        return null;
    }

    @Override
    public String interest() {
        return ChangePeersRequest.class.getName();
    }
}
