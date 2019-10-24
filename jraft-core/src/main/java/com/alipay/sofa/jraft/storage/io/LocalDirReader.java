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
package com.alipay.sofa.jraft.storage.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RetryAgainException;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.google.protobuf.Message;

/**
 * Read a file data form local dir by fileName.
 * 基于本地文件系统存储
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-06 9:25:12 PM
 */
public class LocalDirReader implements FileReader {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDirReader.class);

    /**
     * 目标路径
     */
    private final String        path;

    public LocalDirReader(String path) {
        super();
        this.path = path;
    }

    @Override
    public String getPath() {
        return path;
    }

    /**
     * 读取文件并将结果保存到 bytebuffer 中
     * @param buf      read bytes into this buf
     * @param fileName file name
     * @param offset   the offset of file
     * @param maxCount max read bytes
     * @return
     * @throws IOException
     * @throws RetryAgainException
     */
    @Override
    public int readFile(final ByteBufferCollector buf, final String fileName, final long offset, final long maxCount)
                                                                                                                     throws IOException,
                                                                                                                     RetryAgainException {
        return readFileWithMeta(buf, fileName, null, offset, maxCount);
    }

    /**
     * 读取文件 和 元数据
     * @param buf
     * @param fileName
     * @param fileMeta
     * @param offset
     * @param maxCount
     * @return
     * @throws IOException
     * @throws RetryAgainException
     */
    @SuppressWarnings("unused")
    protected int readFileWithMeta(final ByteBufferCollector buf, final String fileName, final Message fileMeta,
                                   long offset, final long maxCount) throws IOException, RetryAgainException {
        // 首先判断是否需要扩容
        buf.expandIfNecessary();
        // 找到对应的文件
        final String filePath = this.path + File.separator + fileName;
        final File file = new File(filePath);
        try (final FileInputStream input = new FileInputStream(file); final FileChannel fc = input.getChannel()) {
            int totalRead = 0;
            while (true) {
                final int nread = fc.read(buf.getBuffer(), offset);
                // 代表读取到了末尾
                if (nread <= 0) {
                    return EOF;
                }
                // 记录读取的总数
                totalRead += nread;
                // 读取的数据还不足
                if (totalRead < maxCount) {
                    // 没有达到要求的量 返回 EOF
                    if (buf.hasRemaining()) {
                        return EOF;
                    } else {
                        // hasRemaining == false 进行扩容
                        buf.expandAtMost((int) (maxCount - totalRead));
                        offset += nread;
                    }
                } else {
                    final long fsize = file.length();
                    if (fsize < 0) {
                        LOG.warn("Invalid file length {}", filePath);
                        return EOF;
                    }
                    // 这里为什么 返回EOF  刚好读取完也不允许吗???
                    if (fsize == offset + nread) {
                        return EOF;
                    } else {
                        return totalRead;
                    }
                }
            }
        }
    }
}
