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
package com.alipay.sofa.jraft.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.internal.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RetryAgainException;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.storage.io.FileReader;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * File reader service.
 * 文件读取服务对象  用于统一管理
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-30 10:23:13 AM
 */
public final class FileService {

    private static final Logger                   LOG           = LoggerFactory.getLogger(FileService.class);

    /**
     * 单例模式
     */
    private static final FileService              INSTANCE      = new FileService();

    /**
     * key readId  value 文件读取对象
     */
    private final ConcurrentMap<Long, FileReader> fileReaderMap = new ConcurrentHashMap<>();
    /**
     * 该属性是一个随机值  每当有某个 FileReader 添加到它的映射容器中 就会将该值+1 并返回作为 readerId
     */
    private final AtomicLong                      nextId        = new AtomicLong();

    /**
     * Retrieve the singleton instance of FileService.
     * 一般情况下通过单例方式获取
     * @return a fileService instance
     */
    public static FileService getInstance() {
        return INSTANCE;
    }

    @OnlyForTest
    void clear() {
        this.fileReaderMap.clear();
    }

    /**
     * 初始化文件服务对象
     */
    private FileService() {
        // 获取进程id
        final long processId = Utils.getProcessId(ThreadLocalRandom.current().nextLong(10000, Integer.MAX_VALUE));
        final long initialValue = Math.abs(processId << 45 | System.nanoTime() << 17 >> 17);
        // 看来生成了一个类似随机数的值
        this.nextId.set(initialValue);
        LOG.info("Initial file reader id in FileService is {}", initialValue);
    }

    /**
     * Handle GetFileRequest, run the response or set the response with done.
     * 处理获取文件的请求  流程是这样 leader 发起要求follower 拉取快照的请求 同时生成一个read 对象并保存在fileService中 之后将信息发送到了follower follower 创建一个session 对象 会按照
     * 每次拉取的最大长度 从leader上找到对应的reader对象 并拉取数据
     */
    public Message handleGetFile(final GetFileRequest request, final RpcRequestClosure done) {
        // 该请求对象本身无效
        if (request.getCount() <= 0 || request.getOffset() < 0) {
            return RpcResponseFactory.newResponse(RaftError.EREQUEST, "Invalid request: %s", request);
        }
        // 通过id 获取对应的 FileReader 对象 没有找到则抛出异常
        final FileReader reader = this.fileReaderMap.get(request.getReaderId());
        if (reader == null) {
            return RpcResponseFactory.newResponse(RaftError.ENOENT, "Fail to find reader=%d", request.getReaderId());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("GetFile from {} path={} filename={} offset={} count={}",
                done.getBizContext().getRemoteAddress(), reader.getPath(), request.getFilename(), request.getOffset(),
                request.getCount());
        }

        // 分配一个 bytebufferCollector 对象  内部包含一个 bytebuffer 对象
        final ByteBufferCollector dataBuffer = ByteBufferCollector.allocate();
        final GetFileResponse.Builder responseBuilder = GetFileResponse.newBuilder();
        try {
            // 将文件从指定偏移量将数据读取到 buffer 中  返回读取的长度   如果是读取元数据 fileName 就是__raft_snapshot_meta 返回-1代表读取到了文件末尾
            final int read = reader
                .readFile(dataBuffer, request.getFilename(), request.getOffset(), request.getCount());
            responseBuilder.setReadSize(read);
            // 是否读到了末尾
            responseBuilder.setEof(read == FileReader.EOF);
            final ByteBuffer buf = dataBuffer.getBuffer();
            // 反转成读模式
            buf.flip();
            // 如果没数据可读 为 data 设置一个 空结果
            if (!buf.hasRemaining()) {
                // skip empty data
                responseBuilder.setData(ByteString.EMPTY);
            } else {
                // TODO check hole
                responseBuilder.setData(ZeroByteStringHelper.wrap(buf));
            }
            return responseBuilder.build();
        } catch (final RetryAgainException e) {
            return RpcResponseFactory.newResponse(RaftError.EAGAIN,
                "Fail to read from path=%s filename=%s with error: %s", reader.getPath(), request.getFilename(),
                e.getMessage());
        } catch (final IOException e) {
            LOG.error("Fail to read file path={} filename={}", reader.getPath(), request.getFilename(), e);
            return RpcResponseFactory.newResponse(RaftError.EIO, "Fail to read from path=%s filename=%s",
                reader.getPath(), request.getFilename());
        }
    }

    /**
     * Adds a file reader and return it's generated readerId.
     * 注册某个reader
     */
    public long addReader(final FileReader reader) {
        final long readerId = this.nextId.getAndIncrement();
        if (this.fileReaderMap.putIfAbsent(readerId, reader) == null) {
            return readerId;
        } else {
            return -1L;
        }
    }

    /**
     * Remove the reader by readerId.
     */
    public boolean removeReader(final long readerId) {
        return this.fileReaderMap.remove(readerId) != null;
    }
}
