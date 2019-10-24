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
package com.alipay.sofa.jraft.closure;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * Save snapshot closure
 * 保存快照的 回调对象
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:21:30 PM
 */
public interface SaveSnapshotClosure extends Closure {

    /**
     * Starts to save snapshot, returns the writer.
     * 传入集群快照信息 并返回一个writer 对象
     * @param meta metadata of snapshot.
     * @return returns snapshot writer.
     */
    SnapshotWriter start(final SnapshotMeta meta);
}
