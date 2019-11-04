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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.Clock;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;

/**
 * KVStore based on RAFT replica state machine.
 * rhea 模块的 门面类
 * @author jiachun.fjc
 */
public class RaftRawKVStore implements RawKVStore {

    private static final Logger LOG = LoggerFactory.getLogger(RaftRawKVStore.class);

    /**
     * 该存储在哪个节点上
     */
    private final Node          node;
    /**
     * 对应实际的存储层 有RocksDB 和 Memory 2种实现
     */
    private final RawKVStore    kvStore;
    /**
     * 专门用于 读取index的线程池
     */
    private final Executor      readIndexExecutor;

    public RaftRawKVStore(Node node, RawKVStore kvStore, Executor readIndexExecutor) {
        this.node = node;
        this.kvStore = kvStore;
        this.readIndexExecutor = readIndexExecutor;
    }

    /**
     * 通过实际存储层返回迭代器
     * @return
     */
    @Override
    public KVIterator localIterator() {
        return this.kvStore.localIterator();
    }

    @Override
    public void get(final byte[] key, final KVStoreClosure closure) {
        get(key, true, closure);
    }

    /**
     * 根据 key 查询数据 并触发回调
     * @param key  通过key 查询数据
     * @param readOnlySafe  true 代表保证一致性读
     * @param closure
     */
    @Override
    public void get(final byte[] key, final boolean readOnlySafe, final KVStoreClosure closure) {
        // 非线程安全读取 直接 访问store 就可以
        if (!readOnlySafe) {
            this.kvStore.get(key, false, closure);
            return;
        }
        // 获取 index 并触发回调  readIndex 就像是一个全局 协调的方法 能确保 当前集群中只有一个节点进行访问 也就做到了 readOnlySafe
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                // 只有确保 成功的时候才尝试获取
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.get(key, true, closure);
                    return;
                }
                // 通过线程池 异步执行任务
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    // 判断该节点是否是 leader
                    if (isLeader()) {
                        LOG.warn("Fail to [get] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createGet(key), closure);
                    } else {
                        LOG.warn("Fail to [get] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        // 根据 返回的status触发回调
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void multiGet(final List<byte[]> keys, final KVStoreClosure closure) {
        multiGet(keys, true, closure);
    }

    /**
     * 批量拉取数据 基本同上
     * @param keys
     * @param readOnlySafe
     * @param closure
     */
    @Override
    public void multiGet(final List<byte[]> keys, final boolean readOnlySafe, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.multiGet(keys, false, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.multiGet(keys, true, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [multiGet] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createMultiGet(keys), closure);
                    } else {
                        LOG.warn("Fail to [multiGet] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
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

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final boolean returnValue, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.scan(startKey, endKey, limit, false, returnValue, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.scan(startKey, endKey, limit, true, returnValue, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [scan] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createScan(startKey, endKey, limit, returnValue), closure);
                    } else {
                        LOG.warn("Fail to [scan] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        if (step > 0) {
            applyOperation(KVOperation.createGetSequence(seqKey, step), closure);
            return;
        }
        // read-only (step==0)
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.getSequence(seqKey, 0, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [getSequence] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createGetSequence(seqKey, 0), closure);
                    } else {
                        LOG.warn("Fail to [getSequence] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        applyOperation(KVOperation.createResetSequence(seqKey), closure);
    }

    /**
     * 当用户 通过 KVStoreStateMachine.onApply 将任务添加到状态机后会转发到该类
     * @param key
     * @param value
     * @param closure
     */
    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        // 将k v 还原成 KVOperation 后 调用applyOperation
        applyOperation(KVOperation.createPut(key, value), closure);
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createGetAndPut(key, value), closure);
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        applyOperation(KVOperation.createCompareAndPut(key, expect, update), closure);
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createMerge(key, value), closure);
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPutList(entries), closure);
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPutIfAbsent(key, value), closure);
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        // The algorithm relies on the assumption that while there is no
        // synchronized clock across the processes, still the local time in
        // every process flows approximately at the same rate, with an error
        // which is small compared to the auto-release time of the lock.
        acquirer.setLockingTimestamp(Clock.defaultClock().getTime());
        applyOperation(KVOperation.createKeyLockRequest(key, fencingKey, Pair.of(keepLease, acquirer)), closure);
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        applyOperation(KVOperation.createKeyLockReleaseRequest(key, acquirer), closure);
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDelete(key), closure);
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDeleteRange(startKey, endKey), closure);
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDeleteList(keys), closure);
    }

    @Override
    public void execute(final NodeExecutor nodeExecutor, final boolean isLeader, final KVStoreClosure closure) {
        applyOperation(KVOperation.createNodeExecutor(nodeExecutor), closure);
    }

    /**
     * 触发一个指定的操作  当向jraft 写入任务时 必须确保当前写入的节点是 leader
     * @param op
     * @param closure
     */
    private void applyOperation(final KVOperation op, final KVStoreClosure closure) {
        // 必须确保当前节点是leader
        if (!isLeader()) {
            // 注意客户端是如何处理这种error的
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        // 创建一个异步任务 并交由 node 来执行
        final Task task = new Task();
        // 这里使用 protoBuf 作为序列化方式 先不看
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        // 将 op(内部包含 kv  和 回调对象 设置到 task中)
        task.setDone(new KVClosureAdapter(closure, op));
        // 将任务提交到节点中    状态机是什么样的角色 store 又是什么样的角色  如果要实现特定功能要连整个store 一起实现那么该框架并不见得有多方便
        this.node.apply(task);
    }

    private boolean isLeader() {
        return this.node.isLeader();
    }
}
