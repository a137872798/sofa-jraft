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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.rhea.errors.Errors;

/**
 * 基于KV 的 回调
 * @author jiachun.fjc
 */
public interface KVStoreClosure extends Closure {

    /**
     * 获取异常信息
     * @return
     */
    Errors getError();

    /**
     * 设置异常
     * @param error
     */
    void setError(final Errors error);

    Object getData();

    void setData(final Object data);
}
