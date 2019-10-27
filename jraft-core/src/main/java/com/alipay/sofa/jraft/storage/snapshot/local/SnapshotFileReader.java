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
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.error.RetryAgainException;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.io.LocalDirReader;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.util.ByteBufferCollector;

/**
 * Snapshot file reader
 * 快照文件读取对象
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 2:03:09 PM
 */
public class SnapshotFileReader extends LocalDirReader {

    /**
     * 读取的过滤器
     */
    private final SnapshotThrottle snapshotThrottle;
    /**
     * 读取后将数据存放到meta中
     */
    private LocalSnapshotMetaTable metaTable;

    public SnapshotFileReader(String path, SnapshotThrottle snapshotThrottle) {
        super(path);
        this.snapshotThrottle = snapshotThrottle;
    }

    public LocalSnapshotMetaTable getMetaTable() {
        return this.metaTable;
    }

    public void setMetaTable(LocalSnapshotMetaTable metaTable) {
        this.metaTable = metaTable;
    }

    public boolean open() {
        final File file = new File(getPath());
        return file.exists();
    }

    /**
     * 从指定偏移量读取数据并保存到 buffer中
     * @param metaBufferCollector
     * @param fileName file name
     * @param offset   the offset of file
     * @param maxCount max read bytes
     * @return
     * @throws IOException
     * @throws RetryAgainException
     */
    @Override
    public int readFile(final ByteBufferCollector metaBufferCollector, final String fileName, final long offset,
                        final long maxCount) throws IOException, RetryAgainException {
        // read the whole meta file.
        if (fileName.equals(Snapshot.JRAFT_SNAPSHOT_META_FILE)) {
            // 这里将metaTable 的 meta 和 fileMap 的数据全部保存到 buffer 中
            final ByteBuffer metaBuf = this.metaTable.saveToByteBufferAsRemote();
            // because bufRef will flip the buffer before using, so we must set the meta buffer position to it's limit.
            // 创建 buffer 时 pos 还是0 因为之后要采用写入模式 为了避免覆盖数据 将pos 定位到 末尾
            metaBuf.position(metaBuf.limit());
            metaBufferCollector.setBuffer(metaBuf);
            // EOF 代表读取到了末尾  end of file
            return EOF;
        }
        // 如果不是meta文件 就从 fileMap 中找到某个指定的文件
        final LocalFileMeta fileMeta = this.metaTable.getFileMeta(fileName);
        if (fileMeta == null) {
            throw new FileNotFoundException("LocalFileMeta not found for " + fileName);
        }

        // go through throttle
        long newMaxCount = maxCount;
        if (this.snapshotThrottle != null) {
            newMaxCount = this.snapshotThrottle.throttledByThroughput(maxCount);
            if (newMaxCount < maxCount) {
                // if it's not allowed to read partly or it's allowed but
                // throughput is throttled to 0, try again.
                if (newMaxCount == 0) {
                    throw new RetryAgainException("readFile throttled by throughput");
                }
            }
        }

        // 从fileMeta 中将数据加载到bytebuffer 中 从offset 只读取 maxCount
        return readFileWithMeta(metaBufferCollector, fileName, fileMeta, offset, newMaxCount);
    }
}
