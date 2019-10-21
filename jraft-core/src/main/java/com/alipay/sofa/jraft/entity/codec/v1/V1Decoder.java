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
package com.alipay.sofa.jraft.entity.codec.v1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Bits;

/**
 * V1 log entry decoder
 * V1版本的解码器
 * @author boyan(boyan@antfin.com)
 *
 */
public final class V1Decoder implements LogEntryDecoder {

    private V1Decoder() {
    }

    public static V1Decoder INSTANCE = new V1Decoder();

    @Override
    public LogEntry decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return null;
        }
        // 如果首字节不是魔数 无法解析
        if (content[0] != LogEntryV1CodecFactory.MAGIC) {
            // Corrupted log
            return null;
        }
        // 创建一个 LogEntry (该对象是写入 leader/follow 的最小单位)
        LogEntry log = new LogEntry();
        // 将数据填充到log中
        decode(log, content);

        return log;
    }

    /**
     * 将 content 的数据 填充到log 中
     * @param log
     * @param content
     */
    public void decode(final LogEntry log, final byte[] content) {
        // 1-5 type
        // 以1 为起点 解析4个字节 并转换成int类型 该值代表长度
        final int iType = Bits.getInt(content, 1);
        // 获取数据体类型
        log.setType(EnumOutter.EntryType.forNumber(iType));
        // 5-13 index
        // 13-21 term
        final long index = Bits.getLong(content, 5);
        final long term = Bits.getLong(content, 13);
        // 将抽取出来的数据转换成 LogId
        log.setId(new LogId(index, term));
        // 21-25 peer count  当前总节点数???  还是除该节点外其他节点???  之后的数据会根据该数值进行循环解析
        int peerCount = Bits.getInt(content, 21);
        // peers
        int pos = 25;
        if (peerCount > 0) {
            List<PeerId> peers = new ArrayList<>(peerCount);
            while (peerCount-- > 0) {
                final short len = Bits.getShort(content, pos);
                final byte[] bs = new byte[len];
                // 2 代表short的长度
                System.arraycopy(content, pos + 2, bs, 0, len);
                // peer len (short in 2 bytes)
                // peer str
                pos += 2 + len;
                final PeerId peer = new PeerId();
                peer.parse(AsciiStringUtil.unsafeDecode(bs));
                peers.add(peer);
            }
            log.setPeers(peers);
        }
        // old peers  尝试解析老节点
        int oldPeerCount = Bits.getInt(content, pos);
        pos += 4;
        if (oldPeerCount > 0) {
            List<PeerId> oldPeers = new ArrayList<>(oldPeerCount);
            while (oldPeerCount-- > 0) {
                final short len = Bits.getShort(content, pos);
                final byte[] bs = new byte[len];
                System.arraycopy(content, pos + 2, bs, 0, len);
                // peer len (short in 2 bytes)
                // peer str
                pos += 2 + len;
                final PeerId peer = new PeerId();
                peer.parse(AsciiStringUtil.unsafeDecode(bs));
                oldPeers.add(peer);
            }
            log.setOldPeers(oldPeers);
        }

        // data  代表还有内容 判定是 数据体 那么为什么不直接按照 type 来解析呢
        if (content.length > pos) {
            final int len = content.length - pos;
            // 分配指定大小
            ByteBuffer data = ByteBuffer.allocate(len);
            // 写模式
            data.put(content, pos, len);
            data.flip();
            // 读模式
            log.setData(data);
        }
    }
}