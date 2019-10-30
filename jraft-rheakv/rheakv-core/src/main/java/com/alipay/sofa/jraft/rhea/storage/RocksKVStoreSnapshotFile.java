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
package com.alipay.sofa.jraft.rhea.storage;

import com.alipay.sofa.jraft.rhea.errors.StorageException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.util.RegionHelper;

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;

/**
 * 基于 rocksDB 的快照文件存储
 * @author jiachun.fjc
 */
public class RocksKVStoreSnapshotFile extends AbstractKVStoreSnapshotFile {

    /**
     * 快照存储对象
     */
    private final RocksRawKVStore kvStore;

    RocksKVStoreSnapshotFile(RocksRawKVStore kvStore) {
        this.kvStore = kvStore;
    }

    /**
     * 存储快照
     * @param snapshotPath
     * @param region
     * @return
     * @throws Exception
     */
    @Override
    LocalFileMeta doSnapshotSave(final String snapshotPath, final Region region) throws Exception {
        if (RegionHelper.isMultiGroup(region)) {
            // 多组 写入到 sst 中 sst是 rocksDB 的一个概念
            this.kvStore.writeSstSnapshot(snapshotPath, region);
            return buildMetadata(region);
        }
        // 如果要求是 快速快照 就生成一个普通的快照
        if (this.kvStore.isFastSnapshot()) {
            this.kvStore.writeSnapshot(snapshotPath);
            return null;
        }
        // 生成备份数据
        final RocksDBBackupInfo backupInfo = this.kvStore.backupDB(snapshotPath);
        return buildMetadata(backupInfo);
    }

    /**
     * 跟上面类似 获取快照信息
     * @param snapshotPath
     * @param meta
     * @param region
     * @throws Exception
     */
    @Override
    void doSnapshotLoad(final String snapshotPath, final LocalFileMeta meta, final Region region) throws Exception {
        if (RegionHelper.isMultiGroup(region)) {
            final Region snapshotRegion = readMetadata(meta, Region.class);
            if (!RegionHelper.isSameRange(region, snapshotRegion)) {
                throw new StorageException("Invalid snapshot region: " + snapshotRegion + " current region is: "
                                           + region);
            }
            this.kvStore.readSstSnapshot(snapshotPath);
            return;
        }
        if (this.kvStore.isFastSnapshot()) {
            this.kvStore.readSnapshot(snapshotPath);
            return;
        }
        final RocksDBBackupInfo rocksBackupInfo = readMetadata(meta, RocksDBBackupInfo.class);
        this.kvStore.restoreBackup(snapshotPath, rocksBackupInfo);
    }
}
