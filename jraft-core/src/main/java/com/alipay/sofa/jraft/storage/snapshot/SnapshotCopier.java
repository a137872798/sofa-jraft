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
package com.alipay.sofa.jraft.storage.snapshot;

import java.io.Closeable;

import com.alipay.sofa.jraft.Status;

/**
 * Copy snapshot from the give resources.
 * 定义快照copier 的 api
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 4:55:26 PM
 */
public abstract class SnapshotCopier extends Status implements Closeable {

    /**
     * Cancel the copy job.
     * 关闭copy 任务
     */
    public abstract void cancel();

    /**
     * Block the thread until this copy job finishes, or some error occurs.
     * 阻塞当前线程直到任务完成
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public abstract void join() throws InterruptedException;

    /**
     * Start the copy job.
     * 开始执行任务
     */
    public abstract void start();

    /**
     * Get the the SnapshotReader which represents the copied Snapshot
     * 获取快照读取对象
     */
    public abstract SnapshotReader getReader();
}
