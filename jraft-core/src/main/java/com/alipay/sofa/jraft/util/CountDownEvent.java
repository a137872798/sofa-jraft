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
package com.alipay.sofa.jraft.util;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CountDown event.
 * 可阻塞等待执行的 event 对象
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-17 1:36:36 PM
 */
public class CountDownEvent {

    private int             state    = 0;
    private final Lock      lock     = new ReentrantLock();
    private final Condition busyCond = lock.newCondition();

    public int incrementAndGet() {
        lock.lock();
        try {
            return ++this.state;
        } finally {
            lock.unlock();
        }
    }

    public void countDown() {
        lock.lock();
        try {
            if (--this.state == 0) {
                busyCond.signalAll();
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void await() throws InterruptedException {
        lock.lock();
        try {
            while (this.state > 0) {
                busyCond.await();
            }
        } finally {
            lock.unlock();
        }
    }
}
