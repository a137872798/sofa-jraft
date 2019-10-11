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

/**
 * Callback closure.
 * 代表关闭时触发的回调
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 11:07:05 AM
 */
public interface Closure {

    /**
     * Called when task is done.
     * 当任务完成时触发的回调对象
     * @param status the task status. 任务结果
     */
    void run(final Status status);
}
