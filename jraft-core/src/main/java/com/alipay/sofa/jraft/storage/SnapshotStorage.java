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

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * Snapshot storage.
 * 快照存储对象
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 3:30:05 PM
 */
public interface SnapshotStorage extends Lifecycle<Void>, Storage {

    /**
     * Set filterBeforeCopyRemote to be true.When true,
     * it will filter the data before copy to remote.
     * 是否要过滤从远端传来的数据
     */
    boolean setFilterBeforeCopyRemote();

    /**
     * Create a snapshot writer.
     * 创建一个 写入对象
     */
    SnapshotWriter create();

    /**
     * Open a snapshot reader.
     * 创建一个快照读取对象
     */
    SnapshotReader open();

    /**
     * Copy data from remote uri.
     * 从远端某个地址 获取 快照读取对象
     * @param uri  remote uri
     * @param opts copy options
     * @return a SnapshotReader instance
     */
    SnapshotReader copyFrom(final String uri, final SnapshotCopierOptions opts);

    /**
     * Starts a copy job to copy data from remote uri.
     * 开始从 远端某个地址 拷贝数据
     * @param uri  remote uri
     * @param opts copy options
     * @return a SnapshotCopier instance
     */
    SnapshotCopier startToCopyFrom(final String uri, final SnapshotCopierOptions opts);

    /**
     * Configure a SnapshotThrottle.
     * 设置阀门
     * @param snapshotThrottle throttle of snapshot
     */
    void setSnapshotThrottle(final SnapshotThrottle snapshotThrottle);
}
