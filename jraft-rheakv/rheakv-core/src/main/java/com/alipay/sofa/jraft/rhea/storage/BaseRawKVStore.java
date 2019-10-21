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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.StorageException;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.codahale.metrics.Timer;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.DB_TIMER;

/**
 * KV 存储骨架类
 * @author jiachun.fjc
 */
public abstract class BaseRawKVStore<T> implements RawKVStore, Lifecycle<T> {

    // 做一些方法的适配

    @Override
    public void get(final byte[] key, final KVStoreClosure closure) {
        get(key, true, closure);
    }

    @Override
    public void multiGet(final List<byte[]> keys, final KVStoreClosure closure) {
        multiGet(keys, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure) {
        scan(startKey, endKey, limit, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, limit, readOnlySafe, true, closure);
    }

    /**
     *
     * @param nodeExecutor  由用户实现 每当状态机触发某个动作时 会调用该方法
     * @param isLeader 当前节点是否是leader 节点
     * @param closure
     */
    @Override
    public void execute(final NodeExecutor nodeExecutor, final boolean isLeader, final KVStoreClosure closure) {
        // 测量相关先不管
        final Timer.Context timeCtx = getTimeContext("EXECUTE");
        try {
            if (nodeExecutor != null) {
                nodeExecutor.execute(Status.OK(), isLeader);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            final Logger LOG = LoggerFactory.getLogger(getClass());
            LOG.error("Fail to [EXECUTE], {}.", StackTraceUtil.stackTrace(e));
            // 执行出现异常
            if (nodeExecutor != null) {
                nodeExecutor.execute(new Status(RaftError.EIO, "Fail to [EXECUTE]"), isLeader);
            }
            // 这里会抛出异常
            setCriticalError(closure, "Fail to [EXECUTE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    /**
     * 步长过大时  返回 MAX_VALUE 否则返回 start + step
     * @param startVal
     * @param step
     * @return
     */
    public long getSafeEndValueForSequence(final long startVal, final int step) {
        return Math.max(startVal, Long.MAX_VALUE - step < startVal ? Long.MAX_VALUE : startVal + step);
    }

    /**
     * Note: This is not a very precise behavior, don't rely on its accuracy.
     * 获取指定的2个key 之间的 key 数量 (非精确)
     */
    public abstract long getApproximateKeysInRange(final byte[] startKey, final byte[] endKey);

    /**
     * Note: This is not a very precise behavior, don't rely on its accuracy.
     * 指定 key 跳过一段距离后返回的key  非精确
     */
    public abstract byte[] jumpOver(final byte[] startKey, final long distance);

    /**
     * Init the fencing token of new region.
     * 生成篱笆 应该是用来隔绝数据的
     * @param parentKey the fencing key of parent region
     * @param childKey  the fencing key of new region
     */
    public abstract void initFencingToken(final byte[] parentKey, final byte[] childKey);

    // static methods
    //
    static Timer.Context getTimeContext(final String opName) {
        return KVMetrics.timer(DB_TIMER, opName).time();
    }

    /**
     * Sets success, if current node is a leader, reply to
     * client success with result data response.
     * 以成功方式触发回调
     * @param closure callback
     * @param data    result data to reply to client   Boolean.true
     */
    static void setSuccess(final KVStoreClosure closure, final Object data) {
        if (closure != null) {
            // closure is null on follower node
            closure.setData(data);
            closure.run(Status.OK());
        }
    }

    /**
     * Sets failure, if current node is a leader, reply to
     * client failure response.
     *
     * @param closure callback
     * @param message error message
     */
    static void setFailure(final KVStoreClosure closure, final String message) {
        if (closure != null) {
            // closure is null on follower node
            closure.setError(Errors.STORAGE_ERROR);
            closure.run(new Status(RaftError.EIO, message));
        }
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closure callback
     * @param message error message
     * @param error   critical error
     */
    static void setCriticalError(final KVStoreClosure closure, final String message, final Throwable error) {
        // Will call closure#run in FSMCaller
        setClosureError(closure);
        if (error != null) {
            throw new StorageException(message, error);
        }
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closures callback list
     * @param message  error message
     * @param error    critical error
     */
    static void setCriticalError(final List<KVStoreClosure> closures, final String message, final Throwable error) {
        for (final KVStoreClosure closure : closures) {
            // Will call closure#run in FSMCaller
            setClosureError(closure);
        }
        if (error != null) {
            throw new StorageException(message, error);
        }
    }

    static void setClosureError(final KVStoreClosure closure) {
        if (closure != null) {
            // closure is null on follower node
            // 设置异常
            closure.setError(Errors.STORAGE_ERROR);
        }
    }

    // 为回调对象设置结果 或者获取结果

    /**
     * Sets the result first, then gets it by {@link #getData(KVStoreClosure)}
     * when we ensure successful.
     *
     * @param closure callback
     * @param data    data
     */
    static void setData(final KVStoreClosure closure, final Object data) {
        if (closure != null) {
            // closure is null on follower node
            closure.setData(data);
        }
    }

    /**
     * Gets data from closure.
     *
     * @param closure callback
     * @return closure data
     */
    static Object getData(final KVStoreClosure closure) {
        return closure == null ? null : closure.getData();
    }
}
