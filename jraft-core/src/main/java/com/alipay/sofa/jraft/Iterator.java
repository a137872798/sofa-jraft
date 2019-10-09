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

import java.nio.ByteBuffer;

/**
 * Iterator over a batch of committed tasks.
 * @see StateMachine#onApply(Iterator)
 * raft 拓展的迭代器对象 实际上用户提交的一系列task 对象会堆积起来一并发送到 stateMachine 上
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 3:20:15 PM
 */
public interface Iterator extends java.util.Iterator<ByteBuffer> {

    // 以下方法的调用一般是按照  iterator.next(), iterator.getData().....

    /**
     * Return the data whose content is the same as what was passed to
     * Node#apply(Task) in the leader node.
     * 代表当前task 对应的数据
     */
    ByteBuffer getData();

    /**
     * Return a unique and monotonically increasing identifier of the current task:
     * - Uniqueness guarantees that committed tasks in different peers with
     *    the same index are always the same and kept unchanged.
     * - Monotonicity guarantees that for any index pair i, j (i < j), task
     *    at index |i| must be applied before task at index |j| in all the
     *    peers from the group.
     *    当前对象对应的下标
     */
    long getIndex();

    /**
     * Returns the term of the leader which to task was applied to.
     * 当前对象是被 term 为多少的 leader 来处理的
     */
    long getTerm();

    /**
     * If done() is non-NULL, you must call done()->Run() after applying this
     * task no matter this operation succeeds or fails, otherwise the
     * corresponding resources would leak.
     *
     * If this task is proposed by this Node when it was the leader of this
     * group and the leadership has not changed before this point, done() is
     * exactly what was passed to Node#apply(Task) which may stand for some
     * continuation (such as respond to the client) after updating the
     * StateMachine with the given task. Otherwise done() must be NULL.
     * */
    Closure done();

    /**
     * Invoked when some critical error occurred. And we will consider the last
     * |ntail| tasks (starting from the last iterated one) as not applied. After
     * this point, no further changes on the StateMachine as well as the Node
     * would be allowed and you should try to repair this replica or just drop it.
     * 
     * If |statInfo| is not NULL, it should describe the detail of the error.
     */
    void setErrorAndRollback(final long ntail, final Status st);
}
