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
package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;

/**
 * Ping request processor.
 * 同步处理对象  这套模板应该跟 RocketMq 一样 一种请求类型对应一个Processor
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 7:33:30 PM
 */
public class PingRequestProcessor extends SyncUserProcessor<PingRequest> {

    /**
     * 对应心跳总是返回OK
     */
    @Override
    public Object handleRequest(BizContext bizCtx, PingRequest request) throws Exception {
        return RpcResponseFactory.newResponse(0, "OK");
    }

    /**
     * 用于处理 PingRequest 请求
     * @return
     */
    @Override
    public String interest() {
        return PingRequest.class.getName();
    }
}
