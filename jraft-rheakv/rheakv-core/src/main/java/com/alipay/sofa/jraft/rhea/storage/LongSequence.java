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

/**
 *
 * @author jiachun.fjc
 */
public abstract class LongSequence {

    /**
     * 基础偏移量???
     */
    private final long base;

    // 序列对象有一个 start 和 end 属性 标明一段范围
    private Sequence   sequence;
    // 该序列的值
    private long       value;

    public LongSequence() {
        this(0);
    }

    public LongSequence(long base) {
        this.base = base;
    }

    public synchronized long get() {
        return this.base + this.value;
    }

    public synchronized long next() {
        if (this.sequence == null) {
            reset();
        }
        final long val = this.value++;
        if (val == this.sequence.getEndValue()) {
            reset();
            return this.base + this.value++;
        }
        return this.base + val;
    }

    private void reset() {
        this.sequence = getNextSequence();
        this.value = sequence.getStartValue();
    }

    public abstract Sequence getNextSequence();
}
