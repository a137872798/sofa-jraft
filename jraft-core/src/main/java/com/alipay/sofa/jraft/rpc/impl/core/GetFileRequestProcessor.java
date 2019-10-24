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

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.storage.FileService;
import com.google.protobuf.Message;

/**
 * Get file request processor.
 * 获取文件请求处理器
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 3:01:25 PM
 */
public class GetFileRequestProcessor extends RpcRequestProcessor<GetFileRequest> {

    public GetFileRequestProcessor(Executor executor) {
        super(executor);
    }

    /**
     * 转发给 文件服务处理
     * @param request
     * @param done
     * @return
     */
    @Override
    public Message processRequest(GetFileRequest request, RpcRequestClosure done) {
        return FileService.getInstance().handleGetFile(request, done);
    }

    @Override
    public String interest() {
        return GetFileRequest.class.getName();
    }
}
