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
package com.alipay.sofa.jraft.storage.snapshot.remote;

import java.io.Closeable;

import com.alipay.sofa.jraft.Status;

/**
 * A copy session.
 * 拷贝过程中产生的会话对象
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 12:02:12 PM
 */
public interface Session extends Closeable {

    /**
     * Cancel the copy job.
     * 可以通过会话对象 关闭该任务
     */
    void cancel();

    /**
     * Block the thread to wait the copy job finishes or canceled.
     * 阻塞线程 直到copy 任务完成
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

    /**
     * Returns the copy job status.
     * 获取拷贝任务当前的状态
     */
    Status status();
}
