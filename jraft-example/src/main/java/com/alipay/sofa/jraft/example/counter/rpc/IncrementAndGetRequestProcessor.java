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
package com.alipay.sofa.jraft.example.counter.rpc;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.example.counter.CounterServer;
import com.alipay.sofa.jraft.example.counter.IncrementAndAddClosure;

/**
 * IncrementAndGetRequest processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:43:57 PM
 */
public class IncrementAndGetRequestProcessor extends AsyncUserProcessor<IncrementAndGetRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementAndGetRequestProcessor.class);

    /**
     * 持有了 CounterServer 的引用  然后CounterServer 中又有 node 和 状态机的引用 这样processor 就可以直接操作状态机了
     */
    private final CounterServer counterServer;

    public IncrementAndGetRequestProcessor(CounterServer counterServer) {
        super();
        this.counterServer = counterServer;
    }

    /**
     * 处理业务请求  注意存在乱序的可能
     * @param bizCtx
     * @param asyncCtx
     * @param request
     */
    @Override
    public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final IncrementAndGetRequest request) {
        // 首先要检测 leader是否已经失效 或者该节点本身就不是leader  因为通过routeTable 获取到的leader 可能已经失效了 而follower 没有察觉 这时当请求已经进入到node 时 需要再判断一次
        if (!this.counterServer.getFsm().isLeader()) {
            // 将本次失败的leaderId 返回
            asyncCtx.sendResponse(this.counterServer.redirect());
            return;
        }

        // 用户每次发起的请求 应该是无序的 而状态机会根据收到的顺序进行处理
        final ValueResponse response = new ValueResponse();
        // 创建一个回调对象  回调对象也需要用户自定义
        final IncrementAndAddClosure closure = new IncrementAndAddClosure(counterServer, request, response,
                status -> {
                    if (!status.isOk()) {
                        response.setErrorMsg(status.getErrorMsg());
                        response.setSuccess(false);
                    }
                    asyncCtx.sendResponse(response);
                });

        try {
            // 需要将入参封装成task
            final Task task = new Task();
            task.setDone(closure);
            task.setData(ByteBuffer
                .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(request)));

            // apply task to raft group.
            // 用户的单个请求被封装成task 对象 并提交到node上
            counterServer.getNode().apply(task);
        } catch (final CodecException e) {
            LOG.error("Fail to encode IncrementAndGetRequest", e);
            response.setSuccess(false);
            response.setErrorMsg(e.getMessage());
            asyncCtx.sendResponse(response);
        }
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}
