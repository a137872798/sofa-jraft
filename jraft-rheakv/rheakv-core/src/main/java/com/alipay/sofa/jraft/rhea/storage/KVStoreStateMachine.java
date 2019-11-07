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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.IllegalKVOperationException;
import com.alipay.sofa.jraft.rhea.errors.StoreCodecException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.RecycleUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;

/**
 * Rhea KV store state machine
 * core 包决定了只要业务方写入自己的逻辑实现状态机执行的任务就具备了分布式一致性
 * @author jiachun.fjc
 */
public class KVStoreStateMachine extends StateMachineAdapter {

    private static final Logger       LOG        = LoggerFactory.getLogger(KVStoreStateMachine.class);

    /**
     * 一组状态监听器对象   实际上 raft群组复制就是通过 实现该接口做到的
     */
    private final List<StateListener> listeners  = new CopyOnWriteArrayList<>();
    /**
     * 当前任期
     */
    private final AtomicLong          leaderTerm = new AtomicLong(-1L);
    /**
     * 业务方传入的数据一般要做序列化处理
     */
    private final Serializer          serializer = Serializers.getDefault();
    /**
     * 在kv store 中 每个store 对象会对应到一个 region
     */
    private final Region              region;
    /**
     * 该对象内部包含多个regionEngine
     */
    private final StoreEngine         storeEngine;
    /**
     * 批量存储对象
     */
    private final BatchRawKVStore<?>  rawKVStore;
    /**
     * 快照文件存储
     */
    private final KVStoreSnapshotFile storeSnapshotFile;
    private final Meter               applyMeter;
    private final Histogram           batchWriteHistogram;

    /**
     * 该状态机对象通过一个engine 进行初始化
     * @param region
     * @param storeEngine
     */
    public KVStoreStateMachine(Region region, StoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
        this.rawKVStore = storeEngine.getRawKVStore();
        this.storeSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.rawKVStore);
        final String regionStr = String.valueOf(this.region.getId());
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, regionStr);
        this.batchWriteHistogram = KVMetrics.histogram(STATE_MACHINE_BATCH_WRITE, regionStr);
    }

    /**
     * 状态机处理一组批量任务
     * 理解整个jraft 框架是怎么使用的 用户通过创建一组任务并提交到jraft 中 会在大部分节点通过请求后 触发成功回调
     * 而群发则是通过group 实现的 该框架是一个纯异步框架 用户通过closure 获取结果
     * @param it  在迭代器中可以存在2种数据体 一种是conf 记录当前集群的状态 一种是用户提交的任务
     */
    @Override
    public void onApply(final Iterator it) {
        int index = 0;
        int applied = 0;
        try {
            // 该对象是 经过优化后的list (ArrayList + Recycle)
            KVStateOutputList kvStates = KVStateOutputList.newInstance();
            // 迭代用户传入的任务
            while (it.hasNext()) {
                KVOperation kvOp;
                // KVClosureAdapter 对象是 KVStore 定制的 所以允许进行强转
                // it 对应 IteratorImpl 对象 通过 done 获取到某个回调对象 (默认有一组列表)
                final KVClosureAdapter done = (KVClosureAdapter) it.done();
                //  回调对象允许为空 如果不为空 会携带一个operation 对象
                if (done != null) {
                    kvOp = done.getOperation();
                } else {
                    // 这里代表没有获取到回调对象
                    final ByteBuffer buf = it.getData();
                    try {
                        // 获取 IteratorImpl 的 data 属性  如果是 byte[] 进行序列化  反之直接序列化整个buf
                        if (buf.hasArray()) {
                            kvOp = this.serializer.readObject(buf.array(), KVOperation.class);
                        } else {
                            kvOp = this.serializer.readObject(buf, KVOperation.class);
                        }
                    } catch (final Throwable t) {
                        ++index;
                        throw new StoreCodecException("Decode operation error", t);
                    }
                }
                // 上面就是从中获取到operation对象

                // ==  arrayList.get(0)
                final KVState first = kvStates.getFirstElement();
                // 应该是上次残留的任务 TODO  思考一下 什么时候会残留任务  应该是使用recycle 的副作用
                // 当op类型不同时代表必须区别处理 所以按照first的类型将 之前余留的任务委托给kvStore 实现
                if (first != null && !first.isSameOp(kvOp)) {
                    // 这里根据 op类型 委托 kvStore执行全部任务
                    applied += batchApplyAndRecycle(first.getOpByte(), kvStates);
                    kvStates = KVStateOutputList.newInstance();
                }
                // 正常情况下 会将 kvop 和回调对象包装成 kvState 并添加到列表中 (转移迭代器的任务到kvStore中)
                kvStates.add(KVState.of(kvOp, done));
                ++index;
                it.next();
            }
            // 将任务提交到 kvStore
            if (!kvStates.isEmpty()) {
                final KVState first = kvStates.getFirstElement();
                assert first != null;
                applied += batchApplyAndRecycle(first.getOpByte(), kvStates);
            }
        } catch (final Throwable t) {
            LOG.error("StateMachine meet critical error: {}.", StackTraceUtil.stackTrace(t));
            it.setErrorAndRollback(index - applied, new Status(RaftError.ESTATEMACHINE,
                "StateMachine meet critical error: %s.", t.getMessage()));
        } finally {
            // metrics: qps
            this.applyMeter.mark(applied);
        }
    }

    /**
     * 批量处理任务
     * @param opByte  代表操作类型
     * @param kvStates   列表中的数据都按照该操作类型执行
     * @return
     */
    private int batchApplyAndRecycle(final byte opByte, final KVStateOutputList kvStates) {
        try {
            final int size = kvStates.size();

            if (size == 0) {
                return 0;
            }

            if (!KVOperation.isValidOp(opByte)) {
                throw new IllegalKVOperationException("Unknown operation: " + opByte);
            }

            // metrics: op qps
            // 测量的先跳过
            final Meter opApplyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, String.valueOf(this.region.getId()),
                KVOperation.opName(opByte));
            opApplyMeter.mark(size);
            this.batchWriteHistogram.update(size);

            // do batch apply
            // 根据 op 确认本次操作请求 并将 kvState中所有任务都委托给kvStore执行
            batchApply(opByte, kvStates);

            return size;
        } finally {
            RecycleUtil.recycle(kvStates);
        }
    }

    /**
     * 委托给  KVStore 执行任务
     * @param opType
     * @param kvStates
     */
    private void batchApply(final byte opType, final KVStateOutputList kvStates) {
        switch (opType) {
            case KVOperation.PUT:
                // 将kvStates 中所有任务都通过put方法设置到rawKVStore 中
                this.rawKVStore.batchPut(kvStates);
                break;
            case KVOperation.PUT_IF_ABSENT:
                this.rawKVStore.batchPutIfAbsent(kvStates);
                break;
            case KVOperation.PUT_LIST:
                this.rawKVStore.batchPutList(kvStates);
                break;
            case KVOperation.DELETE:
                this.rawKVStore.batchDelete(kvStates);
                break;
            case KVOperation.DELETE_RANGE:
                this.rawKVStore.batchDeleteRange(kvStates);
                break;
            case KVOperation.DELETE_LIST:
                this.rawKVStore.batchDeleteList(kvStates);
                break;
            case KVOperation.GET_SEQUENCE:
                this.rawKVStore.batchGetSequence(kvStates);
                break;
            case KVOperation.NODE_EXECUTE:
                this.rawKVStore.batchNodeExecute(kvStates, isLeader());
                break;
            case KVOperation.KEY_LOCK:
                this.rawKVStore.batchTryLockWith(kvStates);
                break;
            case KVOperation.KEY_LOCK_RELEASE:
                this.rawKVStore.batchReleaseLockWith(kvStates);
                break;
            case KVOperation.GET:
                this.rawKVStore.batchGet(kvStates);
                break;
            case KVOperation.MULTI_GET:
                this.rawKVStore.batchMultiGet(kvStates);
                break;
            case KVOperation.SCAN:
                this.rawKVStore.batchScan(kvStates);
                break;
            case KVOperation.GET_PUT:
                this.rawKVStore.batchGetAndPut(kvStates);
                break;
            case KVOperation.COMPARE_PUT:
                this.rawKVStore.batchCompareAndPut(kvStates);
                break;
            case KVOperation.MERGE:
                this.rawKVStore.batchMerge(kvStates);
                break;
            case KVOperation.RESET_SEQUENCE:
                this.rawKVStore.batchResetSequence(kvStates);
                break;
            // 拆分数据
            case KVOperation.RANGE_SPLIT:
                doSplit(kvStates);
                break;
            default:
                throw new IllegalKVOperationException("Unknown operation: " + opType);
        }
    }

    /**
     * 拆分数据
     * @param kvStates
     */
    private void doSplit(final KVStateOutputList kvStates) {
        final byte[] parentKey = this.region.getStartKey();
        for (final KVState kvState : kvStates) {
            final KVOperation op = kvState.getOp();
            final long currentRegionId = op.getCurrentRegionId();
            final long newRegionId = op.getNewRegionId();
            final byte[] splitKey = op.getKey();
            final KVStoreClosure closure = kvState.getDone();
            try {
                this.rawKVStore.initFencingToken(parentKey, splitKey);
                this.storeEngine.doSplit(currentRegionId, newRegionId, splitKey);
                if (closure != null) {
                    // null on follower
                    closure.setData(Boolean.TRUE);
                    closure.run(Status.OK());
                }
            } catch (final Throwable t) {
                LOG.error("Fail to split, regionId={}, newRegionId={}, splitKey={}.", currentRegionId, newRegionId,
                    BytesUtil.toHex(splitKey));
                setCriticalError(closure, t);
            }
        }
    }

    /**
     * 保存快照  该方法由用户来实现  writer中已经包含了快照文件的路径
     * @param writer
     * @param done
     */
    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        this.storeSnapshotFile.save(writer, done, this.region.copy(), this.storeEngine.getSnapshotExecutor());
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        return this.storeSnapshotFile.load(reader, this.region.copy());
    }

    /**
     * @param term
     */
    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        // 更新任期
        this.leaderTerm.set(term);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        // 使用 storeEngine 专有的 状态变更触发线程池去执行监听器
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onLeaderStart(term);
            }
        });
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        final long oldTerm = this.leaderTerm.get();
        this.leaderTerm.set(-1L);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we asynchronously
        // triggers the listeners.
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onLeaderStop(oldTerm);
            }
        });
    }

    /**
     * 当 某个follower 开始跟随某个leader时 触发 无默认实现  而onLeaderStart 会触发pd 的某个方法
     * @param ctx
     */
    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        // 父类就是打印日志
        super.onStartFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onStartFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    /**
     * follower 停止跟随leader时触发  无默认实现
     * @param ctx
     */
    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onStopFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public void addStateListener(final StateListener listener) {
        this.listeners.add(listener);
    }

    public long getRegionId() {
        return this.region.getId();
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closure callback
     * @param ex      critical error
     */
    private static void setCriticalError(final KVStoreClosure closure, final Throwable ex) {
        // Will call closure#run in FSMCaller
        if (closure != null) {
            closure.setError(Errors.forException(ex));
        }
        ThrowUtil.throwException(ex);
    }
}
