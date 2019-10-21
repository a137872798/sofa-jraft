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
package com.alipay.sofa.jraft.entity;

/**
 * Checksum for entity.
 * 校验和接口
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 */
public interface Checksum {

    /**
     * Caculate a checksum value for this entity.
     * 根据当前数据实体 计算校验和
     * @return checksum value
     */
    long checksum();

    /**
     * Returns the checksum value of two long values.
     * 检查校验和是否正确   咋不直接用 == 呢
     * @param v1 first long value
     * @param v2 second long value
     * @return checksum value
     */
    default long checksum(final long v1, final long v2) {
        return v1 ^ v2;
    }
}
