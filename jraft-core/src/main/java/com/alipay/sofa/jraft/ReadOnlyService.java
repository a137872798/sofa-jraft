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
package com.alipay.sofa.jraft;

import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;

/**
 * The read-only query service.
 * 处理获取 readIndex 相关的请求
 * @author dennis
 *
 */
public interface ReadOnlyService extends Lifecycle<ReadOnlyServiceOptions> {

    /**
     * Adds a ReadIndex request.
     * 增加读取index 的请求
     * @param reqCtx    request context of readIndex
     * @param closure   callback
     */
    void addRequest(final byte[] reqCtx, final ReadIndexClosure closure);

    /**
     * Waits for service shutdown.
     * 等待服务终止
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

}
