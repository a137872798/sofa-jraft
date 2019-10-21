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
package com.alipay.sofa.jraft.entity;

import java.nio.ByteBuffer;
import java.util.List;

import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import com.alipay.sofa.jraft.entity.codec.v1.V1Decoder;
import com.alipay.sofa.jraft.entity.codec.v1.V1Encoder;
import com.alipay.sofa.jraft.util.CrcUtil;

/**
 * A replica log entry.
 * jraft 中的日志实体 就是以该对象的形式存储日志的  该对象还记录了集群的快照 (peers oldPeers)
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:13:02 PM
 */
public class LogEntry implements Checksum {

    /** entry type 代表是 配置信息 或者是 数据(也就是客户端传来的数据 会被当成日志写入到节点中)*/
    private EnumOutter.EntryType type;
    /** log id with index/term 日志id 是由偏移量和 任期组合成的*/
    private LogId                id = new LogId(0, 0);
    /** log entry current peers PeerId 当前集群中节点*/
    private List<PeerId>         peers;
    /** log entry old peers 旧节点*/
    private List<PeerId>         oldPeers;
    /** entry data 数据实体存放在 byteBuffer中 */
    private ByteBuffer           data;
    /** checksum for log entry 校验和*/
    private long                 checksum;
    /** true when the log has checksum 是否包含校验和 **/
    private boolean              hasChecksum;

    public LogEntry() {
        super();
    }

    public LogEntry(final EnumOutter.EntryType type) {
        super();
        this.type = type;
    }

    /**
     * 计算该对象的 校验和
     * @return
     */
    @Override
    public long checksum() {
        long c = this.checksum(this.type.getNumber(), this.id.checksum());
        if (this.peers != null && !this.peers.isEmpty()) {
            for (PeerId peer : this.peers) {
                c = this.checksum(c, peer.checksum());
            }
        }
        if (this.oldPeers != null && !this.oldPeers.isEmpty()) {
            for (PeerId peer : this.oldPeers) {
                c = this.checksum(c, peer.checksum());
            }
        }
        if (this.data != null && this.data.hasRemaining()) {
            byte[] bs = new byte[this.data.remaining()];
            this.data.mark();
            this.data.get(bs);
            this.data.reset();
            c = this.checksum(c, CrcUtil.crc64(bs));
        }
        return c;
    }

    // 现在通过 V1 V2 编解码器  进行编解码了

    /**
     * Please use {@link LogEntryEncoder} instead.
     *
     * @deprecated
     * @return encoded byte array
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public byte[] encode() {
        return V1Encoder.INSTANCE.encode(this);
    }

    /**
     * Please use {@link LogEntryDecoder} instead.
     *
     * @deprecated
     * @return whether success to decode
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public boolean decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return false;
        }
        if (content[0] != LogEntryV1CodecFactory.MAGIC) {
            // Corrupted log
            return false;
        }
        V1Decoder.INSTANCE.decode(this, content);
        return true;
    }

    /**
     * Returns whether the log entry has a checksum.
     * @return true when the log entry has checksum, otherwise returns false.
     * @since 1.2.26
     */
    public boolean hasChecksum() {
        return this.hasChecksum;
    }

    /**
     * Returns true when the log entry is corrupted, it means that the checksum is mismatch.
     * 判断数据是否正确 如果校验和匹配失败 代表数据异常
     * @since 1.2.6
     * @return true when the log entry is corrupted, otherwise returns false
     */
    public boolean isCorrupted() {
        return this.hasChecksum && this.checksum != checksum();
    }

    // get/set 方法

    /**
     * Returns the checksum of the log entry. You should use {@link #hasChecksum} to check if
     * it has checksum.
     * @return checksum value
     */
    public long getChecksum() {
        return this.checksum;
    }

    public void setChecksum(final long checksum) {
        this.checksum = checksum;
        this.hasChecksum = true;
    }

    public EnumOutter.EntryType getType() {
        return this.type;
    }

    public void setType(final EnumOutter.EntryType type) {
        this.type = type;
    }

    public LogId getId() {
        return this.id;
    }

    public void setId(final LogId id) {
        this.id = id;
    }

    public List<PeerId> getPeers() {
        return this.peers;
    }

    public void setPeers(final List<PeerId> peers) {
        this.peers = peers;
    }

    public List<PeerId> getOldPeers() {
        return this.oldPeers;
    }

    public void setOldPeers(final List<PeerId> oldPeers) {
        this.oldPeers = oldPeers;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    public void setData(final ByteBuffer data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "LogEntry [type=" + this.type + ", id=" + this.id + ", peers=" + this.peers + ", oldPeers="
               + this.oldPeers + ", data=" + (this.data != null ? this.data.remaining() : 0) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.data == null ? 0 : this.data.hashCode());
        result = prime * result + (this.id == null ? 0 : this.id.hashCode());
        result = prime * result + (this.oldPeers == null ? 0 : this.oldPeers.hashCode());
        result = prime * result + (this.peers == null ? 0 : this.peers.hashCode());
        result = prime * result + (this.type == null ? 0 : this.type.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LogEntry other = (LogEntry) obj;
        if (this.data == null) {
            if (other.data != null) {
                return false;
            }
        } else if (!this.data.equals(other.data)) {
            return false;
        }
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
            return false;
        }
        if (this.oldPeers == null) {
            if (other.oldPeers != null) {
                return false;
            }
        } else if (!this.oldPeers.equals(other.oldPeers)) {
            return false;
        }
        if (this.peers == null) {
            if (other.peers != null) {
                return false;
            }
        } else if (!this.peers.equals(other.peers)) {
            return false;
        }
        return this.type == other.type;
    }
}
