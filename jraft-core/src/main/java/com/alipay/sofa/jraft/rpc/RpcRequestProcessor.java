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
package com.alipay.sofa.jraft.rpc;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.google.protobuf.Message;

/**
 * Abstract AsyncUserProcessor for RPC processors.
 * 继承异步处理器 也就是有专门的线程池来处理???
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 5:55:39 PM
 * @param <T>
 */
public abstract class RpcRequestProcessor<T extends Message> extends AsyncUserProcessor<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(RpcRequestProcessor.class);

    /**
     * 使用的线程池对象 请求在这里处理 对端通过阻塞方式实现同步 或者使用回调对象+非阻塞
     */
    private final Executor        executor;

    /**
     * 处理请求 返回结果 并触发回调
     * @param request
     * @param done
     * @return
     */
    public abstract Message processRequest(T request, RpcRequestClosure done);

    public RpcRequestProcessor(Executor executor) {
        super();
        this.executor = executor;
    }

    /**
     * 这里是 server 处理client 请求的入口
     * @param bizCtx  存放双端信息
     * @param asyncCtx
     * @param request
     */
    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, T request) {
        try {
            // 处理请求并生成响应结果
            final Message msg = this.processRequest(request, new RpcRequestClosure(bizCtx, asyncCtx));
            if (msg != null) {
                // 如果 response 不为空 则返回给 client  如果下层已经将结果通过回调中的 asyncContext 发送了 那么这里就返回null
                asyncCtx.sendResponse(msg);
            }
        } catch (final Throwable t) {
            LOG.error("handleRequest {} failed", request, t);
            asyncCtx.sendResponse(RpcResponseFactory.newResponse(-1, "handleRequest internal error"));
        }
    }

    @Override
    public Executor getExecutor() {
        return executor;
    }
}
