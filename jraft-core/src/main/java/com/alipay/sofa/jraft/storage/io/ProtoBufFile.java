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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.alipay.sofa.jraft.rpc.ProtobufMsgFactory;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * A file to store protobuf message. Format:
 * <ul>
 * <li>class name length(4 bytes)</li>
 * <li>class name</li>
 * <li> msg length(4 bytes)</li>
 * <li>msg data</li>
 * </ul>
 * @author boyan (boyan@alibaba-inc.com)
 * 按照特定格式存储数据的文件
 * 2018-Mar-12 8:56:23 PM
 */
public class ProtoBufFile {

    static {
        ProtobufMsgFactory.load();
    }

    /** file path 文件路径*/
    private final String path;

    public ProtoBufFile(final String path) {
        this.path = path;
    }

    /**
     * Load a protobuf message from file.
     */
    public <T extends Message> T load() throws IOException {
        File file = new File(this.path);

        if (!file.exists()) {
            return null;
        }

        // 先申请 标明长度用的数组
        final byte[] lenBytes = new byte[4];
        try (final FileInputStream fin = new FileInputStream(file);
                final BufferedInputStream input = new BufferedInputStream(fin)) {
            // 读取长度
            readBytes(lenBytes, input);
            final int len = Bits.getInt(lenBytes, 0);
            if (len <= 0) {
                throw new IOException("Invalid message fullName.");
            }
            final byte[] nameBytes = new byte[len];
            readBytes(nameBytes, input);
            final String name = new String(nameBytes);
            readBytes(lenBytes, input);
            final int msgLen = Bits.getInt(lenBytes, 0);
            final byte[] msgBytes = new byte[msgLen];
            readBytes(msgBytes, input);
            // 将数据按照特定格式解析
            return ProtobufMsgFactory.newMessageByProtoClassName(name, msgBytes);
        }
    }

    private void readBytes(final byte[] bs, final InputStream input) throws IOException {
        int read;
        if ((read = input.read(bs)) != bs.length) {
            throw new IOException("Read error, expects " + bs.length + " bytes, but read " + read);
        }
    }

    /**
     * Save a protobuf message to file.
     *
     * @param msg  protobuf message  准备写入的数据
     * @param sync if sync flush data to disk  是否同步写入
     * @return true if save success
     */
    public boolean save(final Message msg, final boolean sync) throws IOException {
        // Write message into temp file
        // 生成临时文件
        final File file = new File(this.path + ".tmp");
        try (final FileOutputStream fOut = new FileOutputStream(file);
                final BufferedOutputStream output = new BufferedOutputStream(fOut)) {
            final byte[] lenBytes = new byte[4];

            // name len + name
            final String fullName = msg.getDescriptorForType().getFullName();
            final int nameLen = fullName.length();
            // 写入长度
            Bits.putInt(lenBytes, 0, nameLen);
            output.write(lenBytes);
            // 写入数据
            output.write(fullName.getBytes());
            // msg len + msg
            final int msgLen = msg.getSerializedSize();
            Bits.putInt(lenBytes, 0, msgLen);
            output.write(lenBytes);
            msg.writeTo(output);
            if (sync) {
                // 确保写入到物理介质中才会返回 看来写入文件实际上操作系统都是按照异步方式写入的
                fOut.getFD().sync();
            }
        }

        // 将 tmp 文件的内容复制到  特殊后缀文件中
        return Utils.atomicMoveFile(file, new File(this.path));
    }
}
