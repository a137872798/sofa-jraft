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

import java.util.concurrent.ExecutorService;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * 基于快照的文件存储
 * @author jiachun.fjc
 */
public interface KVStoreSnapshotFile {

    /**
     * Save a snapshot for the specified region.
     * 为特定的 region 存储快照
     * @param writer   snapshot writer  写入快照的对象
     * @param done     callback
     * @param region   the region to save snapshot
     * @param executor the executor to compress snapshot  使用特定的线程池实现异步写入
     */
    void save(final SnapshotWriter writer, final Closure done, final Region region, final ExecutorService executor);

    /**
     * Load snapshot for the specified region.
     * 加载某个指定地区的 快照信息
     * @param reader snapshot reader
     * @param region the region to load snapshot
     * @return true if load succeed
     */
    boolean load(final SnapshotReader reader, final Region region);
}
