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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * The iterator implementation.
 * 迭代器实现类
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 3:28:37 PM
 */
public class IteratorImpl {

    /**
     * 该迭代器内部维护了 被提交到的 状态机对象
     */
    private final StateMachine  fsm;
    /**
     * 该对象用于与  LogStorage 进行交互
     */
    private final LogManager    logManager;
    /**
     * 内部存放一组待处理的对象  实际就是被迭代的实体
     */
    private final List<Closure> closures;
    private final long          firstClosureIndex;
    /**
     * 当前指针指向迭代器的哪个位置
     */
    private long                currentIndex;
    private final long          committedIndex;
    /**
     * 该对象记录了当前被迭代到的位置
     */
    private LogEntry            currEntry = new LogEntry(); // blank entry
    /**
     * 等同于currentIndex
     */
    private final AtomicLong    applyingIndex;
    private RaftException       error;

    /**
     *
     * @param fsm   状态机
     * @param logManager   保存LogEntry 的对象
     * @param closures   一组待处理的回调对象
     * @param firstClosureIndex  代表从哪个下标开始有回调对象  看来不是每个数据体都会包含回调 也许有些任务是不需要回调的
     * @param lastAppliedIndex 代表从哪里开始
     * @param committedIndex  代表 允许提交的最大偏移量 从0 ~ committedIndex 的任务都可以被提交到状态机中
     * @param applyingIndex
     */
    public IteratorImpl(final StateMachine fsm, final LogManager logManager, final List<Closure> closures,
                        final long firstClosureIndex, final long lastAppliedIndex, final long committedIndex,
                        final AtomicLong applyingIndex) {
        super();
        this.fsm = fsm;
        this.logManager = logManager;
        this.closures = closures;
        this.firstClosureIndex = firstClosureIndex;
        this.currentIndex = lastAppliedIndex;
        this.committedIndex = committedIndex;
        this.applyingIndex = applyingIndex;
        next();
    }

    @Override
    public String toString() {
        return "IteratorImpl [fsm=" + this.fsm + ", logManager=" + this.logManager + ", closures=" + this.closures
               + ", firstClosureIndex=" + this.firstClosureIndex + ", currentIndex=" + this.currentIndex
               + ", committedIndex=" + this.committedIndex + ", currEntry=" + this.currEntry + ", applyingIndex="
               + this.applyingIndex + ", error=" + this.error + "]";
    }

    public LogEntry entry() {
        return this.currEntry;
    }

    public RaftException getError() {
        return this.error;
    }

    public boolean isGood() {
        return this.currentIndex <= this.committedIndex && !hasError();
    }

    public boolean hasError() {
        return this.error != null;
    }

    /**
     * Move to next
     * 该对应一般是用户传入到状态机中使用的
     */
    public void next() {
        this.currEntry = null; //release current entry
        //get next entry  这2个值的差值 就是待写入的数据量
        if (this.currentIndex <= this.committedIndex) {
            ++this.currentIndex;
            if (this.currentIndex <= this.committedIndex) {
                try {
                    // 该迭代器的数据一开始就是存放在 logManager 中的??? 那么是什么时候设置进去的
                    this.currEntry = this.logManager.getEntry(this.currentIndex);
                    if (this.currEntry == null) {
                        // 初始化error 属性 这样当调用迭代器的 hasNext 时会返回false
                        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                        getOrCreateError().getStatus().setError(-1,
                            "Fail to get entry at index=%d while committed_index=%d", this.currentIndex,
                            this.committedIndex);
                    }
                    // 代表checkSum 校验失败
                } catch (final LogEntryCorruptedException e) {
                    getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                    getOrCreateError().getStatus().setError(RaftError.EINVAL, e.getMessage());
                }
                // 更新当前处理的偏移量
                this.applyingIndex.set(this.currentIndex);
            }
        }
    }

    public long getIndex() {
        return this.currentIndex;
    }

    public Closure done() {
        if (this.currentIndex < this.firstClosureIndex) {
            return null;
        }
        // 返回 List<Closure> 中对应的元素
        return this.closures.get((int) (this.currentIndex - this.firstClosureIndex));
    }

    protected void runTheRestClosureWithError() {
        for (long i = Math.max(this.currentIndex, this.firstClosureIndex); i <= this.committedIndex; i++) {
            final Closure done = this.closures.get((int) (i - this.firstClosureIndex));
            if (done != null) {
                Requires.requireNonNull(this.error, "error");
                Requires.requireNonNull(this.error.getStatus(), "error.status");
                final Status status = this.error.getStatus();
                Utils.runClosureInThread(done, status);
            }
        }
    }

    public void setErrorAndRollback(final long ntail, final Status st) {
        Requires.requireTrue(ntail > 0, "Invalid ntail=" + ntail);
        if (this.currEntry == null || this.currEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            this.currentIndex -= ntail;
        } else {
            this.currentIndex -= ntail - 1;
        }
        this.currEntry = null;
        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE);
        getOrCreateError().getStatus().setError(RaftError.ESTATEMACHINE,
            "StateMachine meet critical error when applying one or more tasks since index=%d, %s", this.currentIndex,
            st != null ? st.toString() : "none");

    }

    private RaftException getOrCreateError() {
        if (this.error == null) {
            this.error = new RaftException();
        }
        return this.error;
    }
}
