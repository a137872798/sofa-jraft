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

import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.google.protobuf.Message;

/**
 * Handle PreVote and RequestVote requests.
 * 处理预投票和投票
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 6:11:09 PM
 */
public class RequestVoteRequestProcessor extends NodeRequestProcessor<RequestVoteRequest> {

    public RequestVoteRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * 此时 peerId 代表follower的信息
     * @param request
     * @return
     */
    @Override
    protected String getPeerId(RequestVoteRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(RequestVoteRequest request) {
        return request.getGroupId();
    }

    @Override
    public Message processRequest0(RaftServerService service, RequestVoteRequest request, RpcRequestClosure done) {
        if (request.getPreVote()) {
            return service.handlePreVoteRequest(request);
        } else {
            // 处理正常的投票请求
            return service.handleRequestVoteRequest(request);
        }
    }

    @Override
    public String interest() {
        return RequestVoteRequest.class.getName();
    }
}
