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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.Url;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.exception.InvokeTimeoutException;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.ClientService;
import com.alipay.sofa.jraft.rpc.ProtobufMsgFactory;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.impl.core.JRaftRpcAddressParser;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolMetricSet;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Abstract RPC client service based on bolt.

 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 3:27:33 PM
 */
public abstract class AbstractBoltClientService implements ClientService {

    protected static final Logger   LOG = LoggerFactory.getLogger(AbstractBoltClientService.class);

    static {
        // 加载PB文件
        ProtobufMsgFactory.load();
    }

    /**
     * ali.remoting  模块的类
     */
    protected RpcClient             rpcClient;
    /**
     * 看来有些请求是交由线程池处理的
     */
    protected ThreadPoolExecutor    rpcExecutor;
    /**
     * 包含一系列 初始化需要的参数
     */
    protected RpcOptions            rpcOptions;
    /**
     * 地址解析器
     */
    protected JRaftRpcAddressParser rpcAddressParser;
    protected InvokeContext         defaultInvokeCtx;

    public RpcClient getRpcClient() {
        return this.rpcClient;
    }

    /**
     * 委托给 RPCClient 判断是否连接正常  底层就是判断 channel.isActive   isWritable
     * @param endpoint server address
     * @return
     */
    @Override
    public boolean isConnected(final Endpoint endpoint) {
        return this.rpcClient.checkConnection(endpoint.toString());
    }

    /**
     * 使用 options 进行初始化
     * @param rpcOptions
     * @return
     */
    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        if (this.rpcClient != null) {
            return true;
        }
        this.rpcOptions = rpcOptions;
        this.rpcAddressParser = new JRaftRpcAddressParser();
        this.defaultInvokeCtx = new InvokeContext();
        // crc开关 就是看是否允许检查校验和
        this.defaultInvokeCtx.put(InvokeContext.BOLT_CRC_SWITCH, this.rpcOptions.isEnableRpcChecksum());
        // 使用指定线程数进行初始化
        return initRpcClient(this.rpcOptions.getRpcProcessorThreadPoolSize());
    }

    /**
     * 交由子类实现
     * @param rpcClient
     */
    protected void configRpcClient(final RpcClient rpcClient) {
        // NO-OP
    }

    /**
     * 初始化 RPCClient
     * @param rpcProcessorThreadPoolSize
     * @return
     */
    protected boolean initRpcClient(final int rpcProcessorThreadPoolSize) {
        this.rpcClient = new RpcClient();
        // 配置client
        configRpcClient(this.rpcClient);
        this.rpcClient.init();
        this.rpcExecutor = ThreadPoolUtil.newBuilder() //
            .poolName("JRaft-RPC-Processor") //
            .enableMetric(true) //
            .coreThreads(rpcProcessorThreadPoolSize / 3) //
            .maximumThreads(rpcProcessorThreadPoolSize) //
            .keepAliveSeconds(60L) //
            .workQueue(new ArrayBlockingQueue<>(10000)) //
            .threadFactory(new NamedThreadFactory("JRaft-RPC-Processor-", true)) //
            .build();
        if (this.rpcOptions.getMetricRegistry() != null) {
            this.rpcOptions.getMetricRegistry().register("raft-rpc-client-thread-pool",
                new ThreadPoolMetricSet(this.rpcExecutor));
            Utils.registerClosureExecutorMetrics(this.rpcOptions.getMetricRegistry());
        }
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.rpcClient != null) {
            this.rpcClient.shutdown();
            this.rpcClient = null;
            this.rpcExecutor.shutdown();
        }
    }

    /**
     * 连接到指定的 端点
     * @param endpoint server address
     * @return
     */
    @Override
    public boolean connect(final Endpoint endpoint) {
        if (this.rpcClient == null) {
            throw new IllegalStateException("Client service is not inited.");
        }
        // 已连接情况下不做处理
        if (isConnected(endpoint)) {
            return true;
        }
        try {
            // 构建一个心跳对象
            final PingRequest req = PingRequest.newBuilder() //
                .setSendTimestamp(System.currentTimeMillis()) //
                .build();
            // 同步发送心跳请求
            final ErrorResponse resp = (ErrorResponse) this.rpcClient.invokeSync(endpoint.toString(), req,
                this.defaultInvokeCtx, this.rpcOptions.getRpcConnectTimeoutMs());
            // 代表成功
            return resp.getErrorCode() == 0;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (final RemotingException e) {
            LOG.error("Fail to connect {}, remoting exception: {}.", endpoint, e.getMessage());
            return false;
        }
    }

    /**
     * 断开连接 底层通过netty
     * @param endpoint server address
     * @return
     */
    @Override
    public boolean disconnect(final Endpoint endpoint) {
        LOG.info("Disconnect from {}", endpoint);
        this.rpcClient.closeConnection(endpoint.toString());
        return true;
    }

    @Override
    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final RpcResponseClosure<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, this.defaultInvokeCtx, done, timeoutMs, this.rpcExecutor);
    }

    /**
     * 当没有指定调用上下文时 使用默认的 上下文对象
     * 实际上上下文对象就是一个map 他会和本次需要发送的数据一起序列化并被发送到对端 对端从中获取元数据信息
     * defaultInvokeCtx 默认情况下只会设置 是否校验crc
     * @param endpoint
     * @param request
     * @param done
     * @param timeoutMs
     * @param rpcExecutor
     * @param <T>
     * @return
     */
    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final RpcResponseClosure<T> done, final int timeoutMs,
                                                              final Executor rpcExecutor) {
        return invokeWithDone(endpoint, request, this.defaultInvokeCtx, done, timeoutMs, rpcExecutor);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final InvokeContext ctx,
                                                              final RpcResponseClosure<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, ctx, done, timeoutMs, this.rpcExecutor);
    }

    /**
     * 发送req 并执行回调
     * @param endpoint
     * @param request
     * @param ctx
     * @param done
     * @param timeoutMs
     * @param rpcExecutor
     * @param <T>
     * @return
     */
    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final InvokeContext ctx,
                                                              final RpcResponseClosure<T> done, final int timeoutMs,
                                                              final Executor rpcExecutor) {
        final FutureImpl<Message> future = new FutureImpl<>();
        try {
            final Url rpcUrl = this.rpcAddressParser.parse(endpoint.toString());
            // 通过 alipay.remoting 的client 发送请求
            this.rpcClient.invokeWithCallback(rpcUrl, request, ctx, new InvokeCallback() {

                // 在回调中嵌套对回调的调用
                @SuppressWarnings("unchecked")
                @Override
                public void onResponse(final Object result) {
                    // 如果future 对象已经被关闭了
                    if (future.isCancelled()) {
                        onCanceled(request, done);
                        return;
                    }
                    // 根据结果设置status 回调中需要该值
                    Status status = Status.OK();
                    if (result instanceof ErrorResponse) {
                        final ErrorResponse eResp = (ErrorResponse) result;
                        status = new Status();
                        status.setCode(eResp.getErrorCode());
                        if (eResp.hasErrorMsg()) {
                            status.setErrorMsg(eResp.getErrorMsg());
                        }
                    } else {
                        // 为回调对象设置结果
                        if (done != null) {
                            done.setResponse((T) result);
                        }
                    }
                    if (done != null) {
                        try {
                            done.run(status);
                        } catch (final Throwable t) {
                            LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                        }
                    }
                    if (!future.isDone()) {
                        // 这里将group 新旧节点信息返回给请求方
                        future.setResult((Message) result);
                    }
                }

                @Override
                public void onException(final Throwable e) {
                    if (future.isCancelled()) {
                        onCanceled(request, done);
                        return;
                    }
                    if (done != null) {
                        try {
                            done.run(new Status(e instanceof InvokeTimeoutException ? RaftError.ETIMEDOUT
                                : RaftError.EINTERNAL, "RPC exception:" + e.getMessage()));
                        } catch (final Throwable t) {
                            LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                        }
                    }
                    if (!future.isDone()) {
                        future.failure(e);
                    }
                }

                @Override
                public Executor getExecutor() {
                    return rpcExecutor != null ? rpcExecutor : AbstractBoltClientService.this.rpcExecutor;
                }
                // 一定会有超时限制的 如果 为-1 就使用默认值
            }, timeoutMs <= 0 ? this.rpcOptions.getRpcDefaultTimeout() : timeoutMs);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            future.failure(e);
            // should be in another thread to avoid dead locking.
            Utils.runClosureInThread(done, new Status(RaftError.EINTR, "Sending rpc was interrupted"));
        } catch (final RemotingException e) {
            future.failure(e);
            // should be in another thread to avoid dead locking.
            Utils.runClosureInThread(done,
                new Status(RaftError.EINTERNAL, "Fail to send a RPC request:" + e.getMessage()));

        }
        return future;
    }

    /**
     * 当发现 future 已经被关闭时 以关闭方式触发回调
     * @param request
     * @param done
     * @param <T>
     */
    private <T extends Message> void onCanceled(final Message request, final RpcResponseClosure<T> done) {
        if (done != null) {
            try {
                done.run(new Status(RaftError.ECANCELED, "RPC request was canceled by future."));
            } catch (final Throwable t) {
                LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
            }
        }
    }

}
