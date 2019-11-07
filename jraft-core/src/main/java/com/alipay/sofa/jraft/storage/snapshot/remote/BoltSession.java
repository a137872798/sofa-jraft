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
package com.alipay.sofa.jraft.storage.snapshot.remote;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CopyOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Copy session based on bolt framework.
 * 该对象必须基于线程安全来实现   该对象封装了 从远端拉取数据的逻辑
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 12:01:23 PM
 */
@ThreadSafe
public class BoltSession implements Session {

    private static final Logger          LOG         = LoggerFactory.getLogger(BoltSession.class);

    private final Lock                   lock        = new ReentrantLock();
    private final Status                 st          = Status.OK();
    /**
     * 用于阻塞直到任务完成的 闭锁
     */
    private final CountDownLatch         finishLatch = new CountDownLatch(1);
    /**
     * 获取文件响应结果的回调对象
     */
    private final GetFileResponseClosure done        = new GetFileResponseClosure();
    /**
     * 客户端对象
     */
    private final RaftClientService      rpcService;
    /**
     * 获取文件的请求对象构建器
     */
    private final GetFileRequest.Builder requestBuilder;
    /**
     * 该会话对应的远端地址
     */
    private final Endpoint               endpoint;
    /**
     * 定时器管理器
     */
    private final TimerManager           timerManager;
    /**
     * 阀门对象
     */
    private final SnapshotThrottle       snapshotThrottle;
    private final RaftOptions            raftOptions;
    private int                          retryTimes  = 0;
    private boolean                      finished;
    private ByteBufferCollector          destBuf;
    private CopyOptions                  copyOptions = new CopyOptions();
    /**
     * 用于从远端写入数据的输出流
     */
    private OutputStream                 outputStream;
    private ScheduledFuture<?>           timer;
    private String                       destPath;
    private Future<Message>              rpcCall;

    /**
     * Get file response closure to answer client.
     * 获取文件请求的 回调对象
     * @author boyan (boyan@alibaba-inc.com)
     */
    private class GetFileResponseClosure extends RpcResponseClosureAdapter<GetFileResponse> {

        /**
         * 当返回结果时触发,处理本次远端调用的res
         * @param status the task status. 任务结果
         */
        @Override
        public void run(final Status status) {
            onRpcReturned(status, getResponse());
        }
    }

    public void setDestPath(final String destPath) {
        this.destPath = destPath;
    }

    @OnlyForTest
    GetFileResponseClosure getDone() {
        return this.done;
    }

    @OnlyForTest
    Future<Message> getRpcCall() {
        return this.rpcCall;
    }

    @OnlyForTest
    ScheduledFuture<?> getTimer() {
        return this.timer;
    }

    @Override
    public void close() throws IOException {
        this.lock.lock();
        try {
            if (!this.finished) {
                // quietly 的含义就是不处理抛出的异常 而是使用打印日志的方式
                Utils.closeQuietly(this.outputStream);
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 会话对象
     * @param rpcService
     * @param timerManager
     * @param snapshotThrottle
     * @param raftOptions
     * @param rb
     * @param ep
     */
    public BoltSession(final RaftClientService rpcService, final TimerManager timerManager,
                       final SnapshotThrottle snapshotThrottle, final RaftOptions raftOptions,
                       final GetFileRequest.Builder rb, final Endpoint ep) {
        super();
        this.snapshotThrottle = snapshotThrottle;
        this.raftOptions = raftOptions;
        this.timerManager = timerManager;
        this.rpcService = rpcService;
        this.requestBuilder = rb;
        this.endpoint = ep;
    }

    public void setDestBuf(final ByteBufferCollector bufRef) {
        this.destBuf = bufRef;
    }

    public void setCopyOptions(final CopyOptions copyOptions) {
        this.copyOptions = copyOptions;
    }

    public void setOutputStream(final OutputStream out) {
        this.outputStream = out;
    }

    @Override
    public void cancel() {
        this.lock.lock();
        try {
            if (this.finished) {
                return;
            }
            if (this.timer != null) {
                this.timer.cancel(true);
            }
            if (this.rpcCall != null) {
                // 调用该方法 会唤醒 调用get 的线程 同时 结果为CANCELLED 代表本次任务是被关闭的
                this.rpcCall.cancel(true);
            }
            if (this.st.isOk()) {
                this.st.setError(RaftError.ECANCELED, RaftError.ECANCELED.name());
            }
            onFinished();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 外部线程 调用该方法时 被阻塞只有当内部线程完成任务后解开闭锁 才能唤醒外部线程
     * @throws InterruptedException
     */
    @Override
    public void join() throws InterruptedException {
        this.finishLatch.await();
    }

    @Override
    public Status status() {
        return this.st;
    }

    /**
     * 当关闭会话对象时触发
     */
    private void onFinished() {
        if (!this.finished) {
            // 如果 st 已经被设置 且 code != 0 代表在拷贝过程中出现了异常
            if (!this.st.isOk()) {
                LOG.error("Fail to copy data, readerId={} fileName={} offset={} status={}",
                    this.requestBuilder.getReaderId(), this.requestBuilder.getFilename(),
                    this.requestBuilder.getOffset(), this.st);
            }
            // 关闭输出流对象
            if (this.outputStream != null) {
                Utils.closeQuietly(this.outputStream);
                this.outputStream = null;
            }
            if (this.destBuf != null) {
                final ByteBuffer buf = this.destBuf.getBuffer();
                if (buf != null) {
                    // 这里为什么要 反转成 读模式呢
                    buf.flip();
                }
                this.destBuf = null;
            }
            // 解开闭锁
            this.finished = true;
            this.finishLatch.countDown();
        }
    }

    /**
     * 代表是重试 首先任务是由一个定时器执行的 其次定时器在执行任务时又会开启一个新线程执行任务
     */
    private void onTimer() {
        Utils.runInThread(this::sendNextRpc);
    }

    /**
     * 当成功从远端拉取到文件数据后 返回    response.data 内部包含了读取到的数据
     * @param status  代表远端调用的结果
     * @param response  代表本次远端调用的结果
     */
    void onRpcReturned(final Status status, final GetFileResponse response) {
        this.lock.lock();
        try {
            // 如果该对象已经被关闭了 就不再处理  finished 不使用 volatile 修饰的原因是 外部已经使用lock 对象加锁了 Lock 接口本身的语义被加强了
            // 能确保可见性
            if (this.finished) {
                return;
            }
            // 代表本次远端访问失败
            if (!status.isOk()) {
                // Reset count to make next rpc retry the previous one
                this.requestBuilder.setCount(0);
                // 如果 leader reader 已经被关闭这里就可以关闭session 了
                if (status.getCode() == RaftError.ECANCELED.getNumber()) {
                    if (this.st.isOk()) {
                        this.st.setError(status.getCode(), status.getErrorMsg());
                        // 做一些清理工作以及解除闭锁
                        onFinished();
                        return;
                    }
                }

                // Throttled reading failure does not increase _retry_times
                // 返回结果 不为重试 且超过了重试次数
                if (status.getCode() != RaftError.EAGAIN.getNumber()
                        && ++this.retryTimes >= this.copyOptions.getMaxRetry()) {
                    if (this.st.isOk()) {
                        this.st.setError(status.getCode(), status.getErrorMsg());
                        onFinished();
                        return;
                    }
                }
                // 这里代表 没有超过最大重试次数 那么在一定延时后 继续获取
                this.timer = this.timerManager.schedule(this::onTimer, this.copyOptions.getRetryIntervalMs(),
                    TimeUnit.MILLISECONDS);
                return;
            }
            // 每次 成功都会重置重试次数
            this.retryTimes = 0;
            Requires.requireNonNull(response, "response");
            // Reset count to |real_read_size| to make next rpc get the right offset
            // 代表没有读取到末尾  就设置真实读取的长度
            if (!response.getEof()) {
                this.requestBuilder.setCount(response.getReadSize());
            }
            // 如果该会话是通过outputStream 初始化的 代表是读取文件
            if (this.outputStream != null) {
                try {
                    // 该API 是 protoBuf 的 将数据输出到输出流中 而数据流本身 定位了目标地址
                    response.getData().writeTo(this.outputStream);
                } catch (final IOException e) {
                    LOG.error("Fail to write into file {}", this.destPath);
                    this.st.setError(RaftError.EIO, RaftError.EIO.name());
                    onFinished();
                    return;
                }
            } else {
                // 如果没有设置 输出流 就将结果设置到 buffer 中 这里就对应2种输出方式 一种输出到 file 一种输出到 buffer
                this.destBuf.put(response.getData().asReadOnlyByteBuffer());
            }
            // 这里会不断调用sendNextRpc 直到数据拉取完毕 触发 onFinished 这时会唤醒闭锁
            if (response.getEof()) {
                onFinished();
                return;
            }
        } finally {
            this.lock.unlock();
        }
        sendNextRpc();
    }

    /**
     * Send next RPC request to get a piece of file data.
     * 开始发送请求 拉取数据
     */
    void sendNextRpc() {
        this.lock.lock();
        try {
            this.timer = null;
            // requestBuilder 内部维护了 拉取数据的偏移量   一开始offset 和 count 都是0
            final long offset = this.requestBuilder.getOffset() + this.requestBuilder.getCount();
            // 设置中包含每次拉取的 长度  如果是保存到 buf 中那么不限制长度
            final long maxCount = this.destBuf == null ? this.raftOptions.getMaxByteCountPerRpc() : Integer.MAX_VALUE;
            // 设置本次的拉取起点 + 拉取总数
            this.requestBuilder.setOffset(offset).setCount(maxCount).setReadPartly(true);

            // 如果 session 对象已经终止 无法拉取数据
            if (this.finished) {
                return;
            }
            // throttle
            long newMaxCount = maxCount;
            // 如果存在阀门 进行额外处理
            if (this.snapshotThrottle != null) {
                // 修改预备拉取的最大偏移量  这里先不看吧 不是重点
                newMaxCount = this.snapshotThrottle.throttledByThroughput(maxCount);
                if (newMaxCount == 0) {
                    // Reset count to make next rpc retry the previous one
                    this.requestBuilder.setCount(0);
                    // 在一定时间后发起重试 这里面是某种限流算法 可能流量还没有生成(令牌)
                    this.timer = this.timerManager.schedule(this::onTimer, this.copyOptions.getRetryIntervalMs(),
                        TimeUnit.MILLISECONDS);
                    return;
                }
            }
            this.requestBuilder.setCount(newMaxCount);
            final RpcRequests.GetFileRequest request = this.requestBuilder.build();
            LOG.debug("Send get file request {} to peer {}", request, this.endpoint);
            // 构建请求对象 并发起请求 获取文件信息  外部通过访问 rpcCall 来获取结果
            this.rpcCall = this.rpcService.getFile(this.endpoint, request, this.copyOptions.getTimeoutMs(), this.done);
        } finally {
            this.lock.unlock();
        }
    }
}
