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

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.alipay.sofa.jraft.Closure;

/**
 * Basic message structure of jraft, contains:
 * <ul>
 * <li>data: associated  task data</li>
 * <li>done: task closure, called when the data is successfully committed to the raft group.</li>
 * <li>expectedTerm: Reject this task if expectedTerm doesn't match the current term of this Node if the value is not -1, default is -1.</li>
 * </ul>
 * 用户的某个操作会被封装成Task 对象之后通过 leader 传播到其他follower上
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 3:08:12 PM
 */
public class Task implements Serializable {

    private static final long serialVersionUID = 2971309899898274575L;

    /** Associated  task data
     *  数据可以存放在 buffer 中
     * */
    private ByteBuffer        data;
    /** task closure, called when the data is successfully committed to the raft group or failures happen.
     *  用于通知结果的回调对象  客户在 task 中设置自定义的 done 对象 用以异步处理结果
     * */
    private Closure           done;
    /** Reject this task if expectedTerm doesn't match the current term of this Node if the value is not -1, default is -1.
     *  代表预期接受的 term term在jraft 中代表在某轮选举过后所有的节点都会同一展示成一个 term
     *  如果设置了预期值 那么在应用到状态机之前会检查节点的 term是否是预期值 不是的话会拒绝任务
     * */
    private long              expectedTerm     = -1;

    public Task() {
        super();
    }

    /**
     * Creates a task with data/done.
     * 通过 存放任务数据的data 和 一个回调对象来进行初始化
     */
    public Task(ByteBuffer data, Closure done) {
        super();
        this.data = data;
        this.done = done;
    }

    /**
     * Creates a task with data/done/expectedTerm.
     * 相比上面增加一个 预期值
     */
    public Task(ByteBuffer data, Closure done, long expectedTerm) {
        super();
        this.data = data;
        this.done = done;
        this.expectedTerm = expectedTerm;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    public Closure getDone() {
        return this.done;
    }

    public void setDone(Closure done) {
        this.done = done;
    }

    public long getExpectedTerm() {
        return this.expectedTerm;
    }

    public void setExpectedTerm(long expectedTerm) {
        this.expectedTerm = expectedTerm;
    }
}
