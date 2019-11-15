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
package com.alipay.sofa.jraft.core;

import java.nio.ByteBuffer;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;

/**
 * 迭代器包装对象
 */
public class IteratorWrapper implements Iterator {

    /**
     * jraft 特殊的迭代器实现
     */
    private final IteratorImpl impl;

    public IteratorWrapper(IteratorImpl iterImpl) {
        super();
        this.impl = iterImpl;
    }

    /**
     * 代表只处理 data 类型数据  因为该包装器是针对 data类型的
     * @return
     */
    @Override
    public boolean hasNext() {
        // 确保impl 没有到 末尾
        return this.impl.isGood() && this.impl.entry().getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA;
    }

    @Override
    public ByteBuffer next() {
        final ByteBuffer data = getData();
        if (hasNext()) {
            this.impl.next();
        }
        return data;
    }

    @Override
    public ByteBuffer getData() {
        final LogEntry entry = this.impl.entry();
        return entry != null ? entry.getData() : null;
    }

    @Override
    public long getIndex() {
        return this.impl.getIndex();
    }

    @Override
    public long getTerm() {
        return this.impl.entry().getId().getTerm();
    }

    @Override
    public Closure done() {
        return this.impl.done();
    }

    @Override
    public void setErrorAndRollback(final long ntail, final Status st) {
        this.impl.setErrorAndRollback(ntail, st);
    }
}
