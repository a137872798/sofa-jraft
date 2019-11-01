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
package com.alipay.sofa.jraft.storage.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.DisruptorMetricSet;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadHelper;
import com.alipay.sofa.jraft.util.Utils;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * LogManager implementation.
 * 日志管理实现类 该对象 用于协调和 控制 LogStorage 对象
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 4:42:20 PM
 */
public class LogManagerImpl implements LogManager {
    private static final int                                 APPEND_LOG_RETRY_TIMES = 50;

    private static final Logger                              LOG                    = LoggerFactory
                                                                                        .getLogger(LogManagerImpl.class);

    /**
     * 日志存储 对象 实际上是基于 RocksDB 进行存储   (该对象是一款 key/value存储数据库)
     */
    private LogStorage                                       logStorage;
    /**
     * 配置管理器 该对象可以从 LogStorage 中获取最新配置
     */
    private ConfigurationManager                             configManager;
    /**
     * StatusMachine 的调用者
     */
    private FSMCaller                                        fsmCaller;
    /**
     * 读写锁
     */
    private final ReadWriteLock                              lock                   = new ReentrantReadWriteLock();
    private final Lock                                       writeLock              = this.lock.writeLock();
    private final Lock                                       readLock               = this.lock.readLock();
    /**
     * 是否已停止
     */
    private volatile boolean                                 stopped;
    /**
     * 是否出现异常
     */
    private volatile boolean                                 hasError;
    private long                                             nextWaitId;
    /**
     * 刷盘的id
     */
    private LogId                                            diskId                 = new LogId(0, 0);
    /**
     * 代表快照的偏移量
     */
    private LogId                                            appliedId              = new LogId(0, 0);
    // TODO  use a lock-free concurrent list instead?
    // 暂存日志的 内存队列
    private ArrayDeque<LogEntry>                             logsInMemory           = new ArrayDeque<>();

    // 记录当前写入日志的 首尾偏移量 避免Log 被重复写入
    private volatile long                                    firstLogIndex;
    private volatile long                                    lastLogIndex;
    /**
     * 记录快照写入 下标
     */
    private volatile LogId                                   lastSnapshotId         = new LogId(0, 0);
    private final Map<Long, WaitMeta>                        waitMap                = new HashMap<>();
    /**
     * 该对象是高性能并发队列中的类
     */
    private Disruptor<StableClosureEvent>                    disruptor;
    /**
     * 高性能队列
     */
    private RingBuffer<StableClosureEvent>                   diskQueue;
    /**
     * 选项信息
     */
    private RaftOptions                                      raftOptions;
    private volatile CountDownLatch                          shutDownLatch;
    /**
     * 统计对象先不看
     */
    private NodeMetrics                                      nodeMetrics;
    /**
     * 日志偏移量监听者
     */
    private final CopyOnWriteArrayList<LastLogIndexListener> lastLogIndexListeners  = new CopyOnWriteArrayList<>();

    /**
     * 代表触发的回调事件类型
     */
    private enum EventType {
        /**
         * 其他类型
         */
        OTHER, // other event type.
        /**
         * 重置类型
         */
        RESET, // reset
        /**
         * 丢弃指定前缀的数据
         */
        TRUNCATE_PREFIX, // truncate log from prefix
        /**
         * 丢弃指定后缀的数据
         */
        TRUNCATE_SUFFIX, // truncate log from suffix
        /**
         * 终止
         */
        SHUTDOWN, //
        /**
         * 获取 最后的 LogEntry 的 id
         */
        LAST_LOG_ID // get last log id
    }

    /**
     * 稳定回调事件  高性能队列就是存放该事件
     */
    private static class StableClosureEvent {
        /**
         * 稳定回调对象
         */
        StableClosure done;
        /**
         * 该回调对应的事件类型
         */
        EventType     type;

        void reset() {
            this.done = null;
            this.type = null;
        }
    }

    /**
     * 稳定回调事件工厂 实现事件工厂  该工厂是disruptor 的接口
     */
    private static class StableClosureEventFactory implements EventFactory<StableClosureEvent> {

        @Override
        public StableClosureEvent newInstance() {
            return new StableClosureEvent();
        }
    }

    /**
     * Waiter metadata
     * 等待处理的数据 会关联对应的回调对象 并保存在一个容器中
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 5:05:04 PM
     */
    private static class WaitMeta {
        /** callback when new log come in  当添加一个新的 LogEntry时触发的回调*/
        NewLogCallback onNewLog;
        /** callback error code 错误码*/
        int            errorCode;
        /** the waiter pass-in argument 处理使用参数*/
        Object         arg;

        public WaitMeta(final NewLogCallback onNewLog, final Object arg, final int errorCode) {
            super();
            this.onNewLog = onNewLog;
            this.arg = arg;
            this.errorCode = errorCode;
        }

    }

    /**
     * 添加 监听器  该监听器 会在lastIndex 发生变化时触发
     * @param listener
     */
    @Override
    public void addLastLogIndexListener(final LastLogIndexListener listener) {
        this.lastLogIndexListeners.add(listener);

    }

    @Override
    public void removeLastLogIndexListener(final LastLogIndexListener listener) {
        this.lastLogIndexListeners.remove(listener);
    }

    /**
     * 对应 LifeCycle的 函数  在NodeImpl 初始化后 会调用该方法 完成LogManager 的初始化 该类相当于一个Log模块的枢纽
     * @param opts
     * @return
     */
    @Override
    public boolean init(final LogManagerOptions opts) {
        this.writeLock.lock();
        try {
            // 日志存储对象必须被初始化
            if (opts.getLogStorage() == null) {
                LOG.error("Fail to init log manager, log storage is null");
                return false;
            }
            // 获取必要的组件
            this.raftOptions = opts.getRaftOptions();
            this.nodeMetrics = opts.getNodeMetrics();
            this.logStorage = opts.getLogStorage();
            this.configManager = opts.getConfigurationManager();

            // 生成一个 日志存储配置对象 并使用该对象进行 LogStorage 的 初始化
            LogStorageOptions lsOpts = new LogStorageOptions();
            lsOpts.setConfigurationManager(this.configManager);
            lsOpts.setLogEntryCodecFactory(opts.getLogEntryCodecFactory());

            // 就是 创建了 rocksDB 相关的 对象 并拉取残存的数据 同时更新 firstLogIndex 和 配置
            if (!this.logStorage.init(lsOpts)) {
                LOG.error("Fail to init logStorage");
                return false;
            }
            // 这时 偏移量已经得到更新了
            this.firstLogIndex = this.logStorage.getFirstLogIndex();
            this.lastLogIndex = this.logStorage.getLastLogIndex();
            // 代表刷盘的起始偏移量
            this.diskId = new LogId(this.lastLogIndex, getTermFromLogStorage(this.lastLogIndex));
            // 获取到状态机对象
            this.fsmCaller = opts.getFsmCaller();
            // 生成高性能并发队列 并设置了一个线程工厂
            this.disruptor = DisruptorBuilder.<StableClosureEvent> newInstance() //
                    .setEventFactory(new StableClosureEventFactory()) // 该工厂总是生成一个 稳定的回调对象
                    .setRingBufferSize(opts.getDisruptorBufferSize()) //
                    .setThreadFactory(new NamedThreadFactory("JRaft-LogManager-Disruptor-", true)) //
                    .setProducerType(ProducerType.MULTI) //
                    /*
                     *  Use timeout strategy in log manager. If timeout happens, it will called reportError to halt the node.
                     */
                    .setWaitStrategy(new TimeoutBlockingWaitStrategy(
                        this.raftOptions.getDisruptorPublishEventWaitTimeoutSecs(), TimeUnit.SECONDS)) // 指定阻塞时长
                    .build();
            // 设置处理事件对应的 handler  该对象就是将生成的事件 写入到 rocksDB 中  什么时候会往队列中插入事件呢???
            this.disruptor.handleEventsWith(new StableClosureEventHandler());
            // 设置异常处理器  异常时 打印日志 且委托给状态机
            this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName(),
                    (event, ex) -> reportError(-1, "LogManager handle event error")));
            // 启动同步队列
            this.diskQueue = this.disruptor.start();
            if(this.nodeMetrics.getMetricRegistry() != null) {
                this.nodeMetrics.getMetricRegistry().register("jraft-log-manager-disruptor", new DisruptorMetricSet(this.diskQueue));
            }
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }

    /**
     * 停止刷盘线程   看来该方法是配合 join() 执行的 join 代表等待 stop 完成 否则是异步关闭
     */
    private void stopDiskThread() {
        this.shutDownLatch = new CountDownLatch(1);
        // 发布一个 reset 事件
        this.diskQueue.publishEvent((event, sequence) -> {
            event.reset();
            event.type = EventType.SHUTDOWN;
        });
    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutDownLatch == null) {
            return;
        }
        // 当 ab.flush 完成时 会释放该锁
        this.shutDownLatch.await();
        this.disruptor.shutdown();
    }

    @Override
    public void shutdown() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            doUnlock = false;
            // 触发所有等待者的 回调方法  看来 并没有直接触发 而是存放到一个map 中 推测有一个额外线程专门处理wait 的回调方法  当shutdown 时 就将缓存的wait全部触发
            wakeupAllWaiter(this.writeLock);
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        // 发布一个 shutdown 事件
        stopDiskThread();
    }

    /**
     * 将 指定id 前的 log 全部移除
     * @param id
     */
    private void clearMemoryLogs(final LogId id) {
        this.writeLock.lock();
        try {
            int index = 0;
            for (final int size = this.logsInMemory.size(); index < size; index++) {
                final LogEntry entry = this.logsInMemory.get(index);
                if (entry.getId().compareTo(id) > 0) {
                    break;
                }
            }
            if (index > 0) {
                this.logsInMemory.removeRange(0, index);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 代表获取最后 日志id 的回调对象
     */
    private static class LastLogIdClosure extends StableClosure {

        public LastLogIdClosure() {
            super(null);
        }

        /**
         * 获取到的最后日志id
         */
        private LogId lastLogId;

        void setLastLogId(final LogId logId) {
            Requires.requireTrue(logId.getIndex() == 0 || logId.getTerm() != 0);
            this.lastLogId = logId;
        }

        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run(final Status status) {
            this.latch.countDown();
        }

        void await() throws InterruptedException {
            this.latch.await();
        }

    }

    /**
     * 操控 logManager  将一组LogEntry 通过该对象存储到 LogStorage
     * @param entries log entries
     * @param done    callback
     */
    @Override
    public void appendEntries(final List<LogEntry> entries, final StableClosure done) {
        Requires.requireNonNull(done, "done");
        // 如果当前  LogManager 处于不可用的状态 使用异常status 来触发回调
        if (this.hasError) {
            entries.clear();
            Utils.runClosureInThread(done, new Status(RaftError.EIO, "Corrupted LogStorage"));
            return;
        }
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 发现要写入的数据异常
            if (!entries.isEmpty() && !checkAndResolveConflict(entries, done)) {
                entries.clear();
                Utils.runClosureInThread(done, new Status(RaftError.EINTERNAL, "Fail to checkAndResolveConflict."));
                return;
            }
            for (int i = 0; i < entries.size(); i++) {
                final LogEntry entry = entries.get(i);
                // Set checksum after checkAndResolveConflict
                if (this.raftOptions.isEnableLogEntryChecksum()) {
                    entry.setChecksum(entry.checksum());
                }
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    Configuration oldConf = new Configuration();
                    if (entry.getOldPeers() != null) {
                        oldConf = new Configuration(entry.getOldPeers());
                    }
                    // configManager 用于管理写入的 conf 信息 同时该对象在初始化时 会从LogStore中加载往期数据 并保存到 conf中
                    final ConfigurationEntry conf = new ConfigurationEntry(entry.getId(),
                        new Configuration(entry.getPeers()), oldConf);
                    this.configManager.add(conf);
                }
            }
            if (!entries.isEmpty()) {
                done.setFirstLogIndex(entries.get(0).getId().getIndex());
                // 数据不是直接写入到 logStore 中的 而是先写到内存中 在合适的时机进行刷盘
                this.logsInMemory.addAll(entries);
            }
            // 将检查冲突完后的数据设置到 done 中
            done.setEntries(entries);

            int retryTimes = 0;
            // 这里是发布 OTHER 事件
            final EventTranslator<StableClosureEvent> translator = (event, sequence) -> {
                event.reset();
                event.type = EventType.OTHER;
                event.done = done;
            };
            while (true) {
                // 尝试写入到队列中
                if (tryOfferEvent(done, translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > APPEND_LOG_RETRY_TIMES) {
                        reportError(RaftError.EBUSY.getNumber(), "LogManager is busy, disk queue overload.");
                        return;
                    }
                    // 自旋
                    ThreadHelper.onSpinWait();
                }
            }
            doUnlock = false;
            // 触发回调对象  这时数据不一定写入到 writeMap 中
            if (!wakeupAllWaiter(this.writeLock)) {
                // 如果没有设置 newLog 的 回调对象 才选择触发该监听器
                notifyLastLogIndexListeners();
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 任务入队列
     * @param done
     * @param type
     */
    private void offerEvent(final StableClosure done, final EventType type) {
        if (this.stopped) {
            Utils.runClosureInThread(done, new Status(RaftError.ESTOP, "Log manager is stopped."));
            return;
        }
        this.diskQueue.publishEvent((event, sequence) -> {
            event.reset();
            event.type = type;
            event.done = done;
        });
    }

    /**
     * 尝试将数据写入到队列中
     * @param done
     * @param translator
     * @return
     */
    private boolean tryOfferEvent(final StableClosure done, final EventTranslator<StableClosureEvent> translator) {
        if (this.stopped) {
            Utils.runClosureInThread(done, new Status(RaftError.ESTOP, "Log manager is stopped."));
            return true;
        }
        return this.diskQueue.tryPublishEvent(translator);
    }

    private void notifyLastLogIndexListeners() {
        for (int i = 0; i < this.lastLogIndexListeners.size(); i++) {
            final LastLogIndexListener listener = this.lastLogIndexListeners.get(i);
            if (listener != null) {
                try {
                    listener.onLastLogIndexChanged(this.lastLogIndex);
                } catch (final Exception e) {
                    LOG.error("Fail to notify LastLogIndexListener, listener={}, index={}", listener, this.lastLogIndex);
                }
            }
        }
    }

    /**
     * 触发回调
     * @param lock
     * @return
     */
    private boolean wakeupAllWaiter(final Lock lock) {
        // 如果map 中无数据 直接返回
        if (this.waitMap.isEmpty()) {
            lock.unlock();
            return false;
        }
        final List<WaitMeta> wms = new ArrayList<>(this.waitMap.values());
        // 停止代表由于该对象被关闭 无法正常写入数据  success 代表该对象对应的 logEntry 已经正常写入
        final int errCode = this.stopped ? RaftError.ESTOP.getNumber() : RaftError.SUCCESS.getNumber();
        this.waitMap.clear();
        // 一旦清空该容器就 唤醒锁 是为了 将耗时操作放到锁外
        lock.unlock();

        final int waiterCount = wms.size();
        for (int i = 0; i < waiterCount; i++) {
            final WaitMeta wm = wms.get(i);
            wm.errorCode = errCode;
            // 处理之前的数据
            Utils.runInThread(() -> runOnNewLog(wm));
        }
        return true;
    }

    /**
     * 将一组日志对象写入到LogStorage 中
     * @param toAppend
     * @return
     */
    private LogId appendToStorage(final List<LogEntry> toAppend) {
        LogId lastId = null;
        // 如果未出现异常 将数据写入
        if (!this.hasError) {
            final long startMs = Utils.monotonicMs();
            final int entriesCount = toAppend.size();
            // TODO 统计逻辑先忽略
            this.nodeMetrics.recordSize("append-logs-count", entriesCount);
            try {
                int writtenSize = 0;
                for (int i = 0; i < entriesCount; i++) {
                    final LogEntry entry = toAppend.get(i);
                    // 累加 写入的长度
                    writtenSize += entry.getData() != null ? entry.getData().remaining() : 0;
                }
                this.nodeMetrics.recordSize("append-logs-bytes", writtenSize);
                // 将数据存储到 storage 中
                final int nAppent = this.logStorage.appendEntries(toAppend);
                // 代表有部分数据写入失败了
                if (nAppent != entriesCount) {
                    LOG.error("**Critical error**, fail to appendEntries, nAppent={}, toAppend={}", nAppent,
                        toAppend.size());
                    // 打印异常信息  EIO 代表  ERROR IO
                    reportError(RaftError.EIO.getNumber(), "Fail to append log entries");
                }
                // 更新 最后的偏移量
                if (nAppent > 0) {
                    lastId = toAppend.get(nAppent - 1).getId();
                }
                toAppend.clear();
            } finally {
                this.nodeMetrics.recordLatency("append-logs", Utils.monotonicMs() - startMs);
            }
        }
        return lastId;
    }

    /**
     * 批量追加对象
     */
    private class AppendBatcher {
        /**
         * 存放 事件的队列 每个 事件中包含一组数据(一个LogEntry 列表)
         */
        List<StableClosure> storage;
        /**
         * 容量大小
         */
        int                 cap;
        /**
         * 数据长度
         */
        int                 size;
        /**
         * 代表缓冲区大小
         */
        int                 bufferSize;
        /**
         * 一组待添加的数据
         */
        List<LogEntry>      toAppend;
        /**
         * 推测是从该偏移量往后添加
         */
        LogId               lastId;

        /**
         * @param storage
         * @param cap  默认为256
         * @param toAppend  默认为空list
         * @param lastId
         */
        public AppendBatcher(final List<StableClosure> storage, final int cap, final List<LogEntry> toAppend,
                             final LogId lastId) {
            super();
            this.storage = storage;
            this.cap = cap;
            this.toAppend = toAppend;
            this.lastId = lastId;
        }

        /**
         * 开始将数据写入到 LogStorage 中
         * @return
         */
        LogId flush() {
            if (this.size > 0) {
                // toAppend 代表一组待写入的数据
                this.lastId = appendToStorage(this.toAppend);
                for (int i = 0; i < this.size; i++) {
                    // storage 中每个元素都对应到toAppend 这里在处理完成时 根据状态触发回调
                    this.storage.get(i).getEntries().clear();
                    if (LogManagerImpl.this.hasError) {
                        this.storage.get(i).run(new Status(RaftError.EIO, "Corrupted LogStorage"));
                    } else {
                        this.storage.get(i).run(Status.OK());
                    }
                }
                this.toAppend.clear();
                this.storage.clear();
            }
            this.size = 0;
            this.bufferSize = 0;
            return this.lastId;
        }

        /**
         * 将事件中的数据 转移到 该对象中
         * @param done
         */
        void append(final StableClosure done) {
            // 如果当前数据已经达到容器大小 就必须先进行一次刷盘  在接收到StableClosure 的 Shutdown 事件时 也会将触发刷盘
            if (this.size == this.cap || this.bufferSize >= LogManagerImpl.this.raftOptions.getMaxAppendBufferSize()) {
                flush();
            }
            // 将数据写入到 storage 中
            this.storage.add(done);
            this.size++;
            // 将数据转移到 toAppend中
            this.toAppend.addAll(done.getEntries());
            for (final LogEntry entry : done.getEntries()) {
                this.bufferSize += entry.getData() != null ? entry.getData().remaining() : 0;
            }
        }
    }

    /**
     * 处理 Disruptor 队列中存放的任务
     */
    private class StableClosureEventHandler implements EventHandler<StableClosureEvent> {
        /**
         * 刷盘id
         */
        LogId               lastId  = LogManagerImpl.this.diskId;
        /**
         * 可以理解为一个快捷引用 实际上跟 appendBatcher 内的 存储列表指向同一对象 内部存放的 StableClosure 会在任务处理完成后触发回调
         */
        List<StableClosure> storage = new ArrayList<>(256);
        /**
         *
         */
        AppendBatcher       ab      = new AppendBatcher(this.storage, 256, new ArrayList<>(),
                                        LogManagerImpl.this.diskId);

        /**
         * 触发事件  从AppendBatch 中可以看出 数据每次都写入  这样避免频繁访问 rocksDB
         * @param event
         * @param sequence
         * @param endOfBatch  通过传入该标识 可以做到 event 没有携带数据的时候也进行刷盘
         * @throws Exception
         */
        @Override
        public void onEvent(final StableClosureEvent event, final long sequence, final boolean endOfBatch)
                                                                                                          throws Exception {
            // 如果是终止任务
            if (event.type == EventType.SHUTDOWN) {
                // 将之前的数据 全部 刷入到 LogStorage 中 不过在这之前要保证storage 中有数据 否则没有数据可以写入
                this.lastId = this.ab.flush();
                // 更新当前要刷盘的起点(id 中携带了 index 和任期)
                setDiskId(this.lastId);
                // 发出 SHUTDOWN 事件时 会设置 闭锁 可以通过 调用wait 配合 countDown 实现同步解锁
                LogManagerImpl.this.shutDownLatch.countDown();
                return;
            }
            // 获取回调对象
            final StableClosure done = event.done;

            // 如果回调对象中存在 要写入的数据 将实体写入到 ab 中
            if (done.getEntries() != null && !done.getEntries().isEmpty()) {
                this.ab.append(done);
            } else {
                // 如果回调事件中携带数据 就存放到appendBatch 中 等积累到一定量才存储
                // 如果没有携带数据 就代表是某种指令
                this.lastId = this.ab.flush();
                boolean ret = true;
                switch (event.type) {
                    // 指令内容为获取最后的 logId
                    case LAST_LOG_ID:
                        // 这里设置到回调对象中
                        ((LastLogIdClosure) done).setLastLogId(this.lastId.copy());
                        break;
                    // 截断 前缀
                    case TRUNCATE_PREFIX:
                        long startMs = Utils.monotonicMs();
                        try {
                            final TruncatePrefixClosure tpc = (TruncatePrefixClosure) done;
                            LOG.debug("Truncating storage to firstIndexKept={}", tpc.firstIndexKept);
                            // 截断数据
                            ret = LogManagerImpl.this.logStorage.truncatePrefix(tpc.firstIndexKept);
                        } finally {
                            LogManagerImpl.this.nodeMetrics.recordLatency("truncate-log-prefix", Utils.monotonicMs()
                                                                                                 - startMs);
                        }
                        break;
                    case TRUNCATE_SUFFIX:
                        startMs = Utils.monotonicMs();
                        try {
                            final TruncateSuffixClosure tsc = (TruncateSuffixClosure) done;
                            LOG.warn("Truncating storage to lastIndexKept={}", tsc.lastIndexKept);
                            ret = LogManagerImpl.this.logStorage.truncateSuffix(tsc.lastIndexKept);
                            if (ret) {
                                // 更新 末尾
                                this.lastId.setIndex(tsc.lastIndexKept);
                                this.lastId.setTerm(tsc.lastTermKept);
                                Requires.requireTrue(this.lastId.getIndex() == 0 || this.lastId.getTerm() != 0);
                            }
                        } finally {
                            LogManagerImpl.this.nodeMetrics.recordLatency("truncate-log-suffix", Utils.monotonicMs()
                                                                                                 - startMs);
                        }
                        break;
                    case RESET:
                        final ResetClosure rc = (ResetClosure) done;
                        LOG.info("Reseting storage to nextLogIndex={}", rc.nextLogIndex);
                        // 重启 rocksDB 并将 偏移量对应的数据重新保存到 DB 中
                        ret = LogManagerImpl.this.logStorage.reset(rc.nextLogIndex);
                        break;
                    default:
                        break;
                }

                if (!ret) {
                    reportError(RaftError.EIO.getNumber(), "Failed operation in LogStorage");
                } else {
                    done.run(Status.OK());
                }
            }
            // endOfBatch 只有当 囤积大量事件未消费时 才有意义 如果 生产/消费速度持平 那么 始终是true
            // 也就是理解为 正常情况下 每次写入的数据 都会立即刷盘  这样确保了数据的实时性
            // 只有当 (Disruptor的概念) 生产者因为什么原因 后面的数据 已经提交了 而前面的还没提交 导致的事件堆积在一次性交由消费者
            // 处理时  这时 每次都刷盘的意义不是很大 因为消费这些数据的事件间隔会很短
            if (endOfBatch) {
                this.lastId = this.ab.flush();
                setDiskId(this.lastId);
            }
        }

    }

    /**
     * 打印异常信息
     * @param code  错误code
     * @param fmt  错误(格式化)信息
     * @param args  参数
     */
    private void reportError(final int code, final String fmt, final Object... args) {
        // 代表出现了问题 这样就不会继续写入日志到 LogStorage 了
        this.hasError = true;
        final RaftException error = new RaftException(ErrorType.ERROR_TYPE_LOG);
        error.setStatus(new Status(code, fmt, args));
        // 使用状态机处理异常
        this.fsmCaller.onError(error);
    }

    /**
     * 更新刷盘id  同时将该偏移量之前的 数据从内存中移除
     * @param id
     */
    private void setDiskId(final LogId id) {
        if (id == null) {
            return;
        }
        LogId clearId;
        this.writeLock.lock();
        try {
            if (id.compareTo(this.diskId) < 0) {
                return;
            }
            this.diskId = id;
            clearId = this.diskId.compareTo(this.appliedId) <= 0 ? this.diskId : this.appliedId;
        } finally {
            this.writeLock.unlock();
        }
        if (clearId != null) {
            clearMemoryLogs(clearId);
        }
    }

    /**
     * 设置快照元数据
     * @param meta snapshot metadata
     */
    @Override
    public void setSnapshot(final SnapshotMeta meta) {
        LOG.debug("set snapshot: {}", meta);
        this.writeLock.lock();
        try {
            // 代表不需要写入
            if (meta.getLastIncludedIndex() <= this.lastSnapshotId.getIndex()) {
                return;
            }
            // 在 jraft中 conf 代表一组 peerId   这里是将meta 的数据转换成 conf
            final Configuration conf = new Configuration();
            for (int i = 0; i < meta.getPeersCount(); i++) {
                final PeerId peer = new PeerId();
                peer.parse(meta.getPeers(i));
                conf.addPeer(peer);
            }
            // 将旧配置相关的数据 转换成oldConf
            final Configuration oldConf = new Configuration();
            for (int i = 0; i < meta.getOldPeersCount(); i++) {
                final PeerId peer = new PeerId();
                peer.parse(meta.getOldPeers(i));
                oldConf.addPeer(peer);
            }

            // 生成配置实体
            final ConfigurationEntry entry = new ConfigurationEntry(new LogId(meta.getLastIncludedIndex(),
                meta.getLastIncludedTerm()), conf, oldConf);
            // 设置配置快照  快照应该不是只包含 新旧集群信息
            this.configManager.setSnapshot(entry);
            // 获取快照数据 对应的任期   这里有一点 如果超过了 lastIndex 那么会返回0
            // 该方法的含义是 当写入快照时 follower 的旧数据就可以清除了 这里是在划分界限 看看对应到哪个任期 如果返回0 代表数据全部过期
            // 如果
            final long term = unsafeGetTerm(meta.getLastIncludedIndex());
            // 代表上次快照写入的 起始偏移量
            final long savedLastSnapshotIndex = this.lastSnapshotId.getIndex();

            // 更新写入的偏移量
            this.lastSnapshotId.setIndex(meta.getLastIncludedIndex());
            this.lastSnapshotId.setTerm(meta.getLastIncludedTerm());

            // appliedId代表快照的偏移量 为什么要保存该变量???
            if (this.lastSnapshotId.compareTo(this.appliedId) > 0) {
                this.appliedId = this.lastSnapshotId.copy();
            }

            // 这里代表  快照对应的任期 超过了 lastIndex 这样可以清除旧数据
            if (term == 0) {
                // last_included_index is larger than last_index
                // FIXME: what if last_included_index is less than first_index?
                // 这里会先将数据从 内存中移除 同时添加一个任务到 Disruptor 中 处理该任务时 会从RocksDB 中移除数据
                truncatePrefix(meta.getLastIncludedIndex() + 1);
            } else if (term == meta.getLastIncludedTerm()) {
                // Truncating log to the index of the last snapshot.
                // We don't truncate log before the last snapshot immediately since
                // some log around last_snapshot_index is probably needed by some
                // followers
                // TODO if there are still be need?
                // 代表 快照之间数据有重叠 这里就删除掉重复的部分
                if (savedLastSnapshotIndex > 0) {
                    truncatePrefix(savedLastSnapshotIndex + 1);
                }
            } else {
                if (!reset(meta.getLastIncludedIndex() + 1)) {
                    LOG.warn("Reset log manager failed, nextLogIndex={}", meta.getLastIncludedIndex() + 1);
                }
            }
        } finally {
            this.writeLock.unlock();
        }

    }

    /**
     * 这里将 快照前的数据 清除掉
     */
    @Override
    public void clearBufferedLogs() {
        this.writeLock.lock();
        try {
            if (this.lastSnapshotId.getIndex() != 0) {
                truncatePrefix(this.lastSnapshotId.getIndex() + 1);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 生成一级缓存的描述信息
     * @return
     */
    private String descLogsInMemory() {
        final StringBuilder sb = new StringBuilder();
        boolean wasFirst = true;
        for (final LogEntry logEntry : this.logsInMemory) {
            if (!wasFirst) {
                sb.append(",");
            } else {
                wasFirst = false;
            }
            sb.append("<id:(").append(logEntry.getId().getTerm()).append(",").append(logEntry.getId().getIndex())
                .append("),type:").append(logEntry.getType()).append(">");
        }
        return sb.toString();
    }

    /**
     * 从一级缓存中获取  存在2种保存方法 一种是 保存到内存中  还有种是保存到 LogStore 中  外部在什么时机 以及按照什么条件来选择保存方式呢???
     * @param index
     * @return
     */
    protected LogEntry getEntryFromMemory(final long index) {
        LogEntry entry = null;
        // 缓存存在的话 就尝试获取
        if (!this.logsInMemory.isEmpty()) {
            final long firstIndex = this.logsInMemory.peekFirst().getId().getIndex();
            final long lastIndex = this.logsInMemory.peekLast().getId().getIndex();
            if (lastIndex - firstIndex + 1 != this.logsInMemory.size()) {
                throw new IllegalStateException(String.format("lastIndex=%d,firstIndex=%d,logsInMemory=[%s]",
                    lastIndex, firstIndex, descLogsInMemory()));
            }
            // 代表在合理的偏移量内
            if (index >= firstIndex && index <= lastIndex) {
                entry = this.logsInMemory.get((int) (index - firstIndex));
            }
        }
        return entry;
    }

    /**
     * 根据指定偏移量从 LogManager 中获取数据实体(每次写入的数据都以 LogEntry 为单位 同时index 每次只增加1)
     * @param index the index of log entry
     * @return
     */
    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (index > this.lastLogIndex || index < this.firstLogIndex) {
                return null;
            }
            // 首先尝试从内存加载
            final LogEntry entry = getEntryFromMemory(index);
            if (entry != null) {
                return entry;
            }
        } finally {
            this.readLock.unlock();
        }
        // 尝试从 logStorage 中获取
        final LogEntry entry = this.logStorage.getEntry(index);
        if (entry == null) {
            // 没有找到抛出异常
            reportError(RaftError.EIO.getNumber(), "Corrupted entry at index=%d, not found", index);
        }
        // Validate checksum
        if (entry != null && this.raftOptions.isEnableLogEntryChecksum() && entry.isCorrupted()) {
            String msg = String.format("Corrupted entry at index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                index, entry.getId().getTerm(), entry.getChecksum(), entry.checksum());
            // Report error to node and throw exception.
            reportError(RaftError.EIO.getNumber(), msg);
            throw new LogEntryCorruptedException(msg);
        }
        return entry;
    }

    @Override
    public long getTerm(final long index) {
        if (index == 0) {
            return 0;
        }
        this.readLock.lock();
        try {
            // out of range, direct return NULL
            if (index > this.lastLogIndex) {
                return 0;
            }
            // check index equal snapshot_index, return snapshot_term
            if (index == this.lastSnapshotId.getIndex()) {
                return this.lastSnapshotId.getTerm();
            }
            // 先尝试从一级缓存获取
            final LogEntry entry = getEntryFromMemory(index);
            if (entry != null) {
                return entry.getId().getTerm();
            }
        } finally {
            this.readLock.unlock();
        }
        // 从LogStorage 中获取
        return getTermFromLogStorage(index);
    }

    /**
     * 从下标中获取任期  就是通过rocksdb 获取logentry (该对象内部封装了任期)
     * 这里没有从缓存中获取
     * @param index
     * @return
     */
    private long getTermFromLogStorage(final long index) {
        LogEntry entry = this.logStorage.getEntry(index);
        if (entry != null) {
            // 是否开启了校验和 且 代表校验失败
            if (this.raftOptions.isEnableLogEntryChecksum() && entry.isCorrupted()) {
                // Report error to node and throw exception.
                String msg = String.format(
                    "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d", entry
                        .getId().getIndex(), entry.getId().getTerm(), entry.getChecksum(), entry.checksum());
                reportError(RaftError.EIO.getNumber(), msg);
                throw new LogEntryCorruptedException(msg);
            }

            return entry.getId().getTerm();
        }
        return 0;
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            return this.firstLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        return getLastLogIndex(false);
    }

    @Override
    public long getLastLogIndex(final boolean isFlush) {
        LastLogIdClosure c;
        this.readLock.lock();
        try {
            if (!isFlush) {
                return this.lastLogIndex;
            } else {
                if (this.lastLogIndex == this.lastSnapshotId.getIndex()) {
                    return this.lastLogIndex;
                }
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            this.readLock.unlock();
        }
        try {
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return c.lastLogId.getIndex();
    }

    private long unsafeGetTerm(final long index) {
        if (index == 0) {
            return 0;
        }
        if (index > this.lastLogIndex) {
            return 0;
        }
        final LogId lss = this.lastSnapshotId;
        if (index == lss.getIndex()) {
            return lss.getTerm();
        }
        final LogEntry entry = getEntryFromMemory(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return getTermFromLogStorage(index);
    }

    /**
     * 获取当前node 写入的最后logId  注意写入的不一定生效 可能leader只是将数据写入少数节点中 而该节点就是少数节点 实际上数据是不作数的
     * @param isFlush whether to flush all pending task.
     * @return
     */
    @Override
    public LogId getLastLogId(final boolean isFlush) {
        LastLogIdClosure c;
        this.readLock.lock();
        try {
            // 非刷盘情况 这里就直接返回LogId 了 看来有个 异步线程在执行刷盘逻辑
            if (!isFlush) {
                if (this.lastLogIndex >= this.firstLogIndex) {
                    return new LogId(this.lastLogIndex, unsafeGetTerm(this.lastLogIndex));
                }
                return this.lastSnapshotId;
            } else {
                if (this.lastLogIndex == this.lastSnapshotId.getIndex()) {
                    return this.lastSnapshotId;
                }
                // 发布获取LastLogId 的事件  eventHandler 在处理不包含数据的 event时都会刷盘
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            this.readLock.unlock();
        }
        try {
            // 等待回调被触发
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        // 获取最新的logId
        return c.lastLogId;
    }

    /**
     * 代表截断前缀的 事件
     */
    private static class TruncatePrefixClosure extends StableClosure {
        /**
         * 截断的偏移量
         */
        long firstIndexKept;

        public TruncatePrefixClosure(final long firstIndexKept) {
            super(null);
            this.firstIndexKept = firstIndexKept;
        }

        /**
         * 回调没有处理
         * @param status the task status. 任务结果
         */
        @Override
        public void run(final Status status) {

        }

    }

    private static class TruncateSuffixClosure extends StableClosure {
        long lastIndexKept;
        long lastTermKept;

        public TruncateSuffixClosure(final long lastIndexKept, final long lastTermKept) {
            super(null);
            this.lastIndexKept = lastIndexKept;
            this.lastTermKept = lastTermKept;
        }

        @Override
        public void run(final Status status) {

        }

    }

    private static class ResetClosure extends StableClosure {
        long nextLogIndex;

        public ResetClosure(final long nextLogIndex) {
            super(null);
            this.nextLogIndex = nextLogIndex;
        }

        @Override
        public void run(final Status status) {

        }
    }

    /**
     * 清除前缀数据
     * @param firstIndexKept
     * @return
     */
    private boolean truncatePrefix(final long firstIndexKept) {
        int index = 0;
        // 先清除一级缓存
        for (final int size = this.logsInMemory.size(); index < size; index++) {
            final LogEntry entry = this.logsInMemory.get(index);
            if (entry.getId().getIndex() >= firstIndexKept) {
                break;
            }
        }
        if (index > 0) {
            this.logsInMemory.removeRange(0, index);
        }

        // TODO  maybe it's fine here
        Requires.requireTrue(firstIndexKept >= this.firstLogIndex,
            "Try to truncate logs before %d, but the firstLogIndex is %d", firstIndexKept, this.firstLogIndex);

        this.firstLogIndex = firstIndexKept;
        if (firstIndexKept > this.lastLogIndex) {
            // The entry log is dropped
            this.lastLogIndex = firstIndexKept - 1;
        }

        LOG.debug("Truncate prefix, firstIndexKept is :{}", firstIndexKept);
        // 将旧的配置也删除  这个 confManager 到底是干嘛用的 因为 加载rockDB 时 将所有conf 信息都保存进去了 如果 过时的数据是无效的
        // 那么一开始就没必要保存该数据
        this.configManager.truncatePrefix(firstIndexKept);
        // 添加写入任务  也就是 针对 rockDB 的操作 都会放到 Disruptor 中
        final TruncatePrefixClosure c = new TruncatePrefixClosure(firstIndexKept);
        offerEvent(c, EventType.TRUNCATE_PREFIX);
        return true;
    }

    private boolean reset(final long nextLogIndex) {
        this.writeLock.lock();
        try {
            this.logsInMemory = new ArrayDeque<>();
            this.firstLogIndex = nextLogIndex;
            this.lastLogIndex = nextLogIndex - 1;
            this.configManager.truncatePrefix(this.firstLogIndex);
            this.configManager.truncateSuffix(this.lastLogIndex);
            final ResetClosure c = new ResetClosure(nextLogIndex);
            offerEvent(c, EventType.RESET);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 丢弃部分数据  一级缓存也要修改
     * @param lastIndexKept
     */
    private void unsafeTruncateSuffix(final long lastIndexKept) {
        if (lastIndexKept < this.appliedId.getIndex()) {
            LOG.error("FATAL ERROR: Can't truncate logs before appliedId={}, lastIndexKept={}", this.appliedId,
                lastIndexKept);
            return;
        }
        while (!this.logsInMemory.isEmpty()) {
            final LogEntry entry = this.logsInMemory.peekLast();
            if (entry.getId().getIndex() > lastIndexKept) {
                this.logsInMemory.pollLast();
            } else {
                break;
            }
        }
        this.lastLogIndex = lastIndexKept;
        // 获取对应的任期
        final long lastTermKept = unsafeGetTerm(lastIndexKept);
        Requires.requireTrue(this.lastLogIndex == 0 || lastTermKept != 0);
        LOG.debug("Truncate suffix :{}", lastIndexKept);
        // 丢弃过期的配置
        this.configManager.truncateSuffix(lastIndexKept);
        final TruncateSuffixClosure c = new TruncateSuffixClosure(lastIndexKept, lastTermKept);
        // 将事件设置到 队列中
        offerEvent(c, EventType.TRUNCATE_SUFFIX);
    }

    /**
     * 检查 和解决冲突    这里传入一个list 就代表日志不是一条条写入的 而是收集后批量写入
     * @param entries  一组待写入的 Log对象 在jraft 中client的任何请求都会被封装成LogEntry对象并保存在节点中
     * @param done
     * @return
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean checkAndResolveConflict(final List<LogEntry> entries, final StableClosure done) {
        // 等同于 entries.get(0)
        final LogEntry firstLogEntry = ArrayDeque.peekFirst(entries);
        // 如果 index ==0 代表无法知晓当前合适的偏移量 这时用之前维护的 lastLogIndex 来设置
        if (firstLogEntry.getId().getIndex() == 0) {
            // Node is currently the leader and |entries| are from the user who
            // don't know the correct indexes the logs should assign to. So we have
            // to assign indexes to the appending entries
            for (int i = 0; i < entries.size(); i++) {
                // 注意这里的 ++
                entries.get(i).getId().setIndex(++this.lastLogIndex);
            }
            return true;
        } else {
            // Node is currently a follower and |entries| are from the leader. We
            // should check and resolve the conflicts between the local logs and
            // |entries|
            // 代表偏移量超过了预期值 无法写入
            if (firstLogEntry.getId().getIndex() > this.lastLogIndex + 1) {
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL,
                    "There's gap between first_index=%d and last_log_index=%d", firstLogEntry.getId().getIndex(),
                    this.lastLogIndex));
                return false;
            }
            // 获取快照偏移量
            final long appliedIndex = this.appliedId.getIndex();
            // 获取最后一个元素
            final LogEntry lastLogEntry = ArrayDeque.peekLast(entries);
            // 如果写入的数据 还在快照的前面 就不需要写入了
            if (lastLogEntry.getId().getIndex() <= appliedIndex) {
                LOG.warn(
                    "Received entries of which the lastLog={} is not greater than appliedIndex={}, return immediately with nothing changed.",
                    lastLogEntry.getId().getIndex(), appliedIndex);
                return false;
            }
            // 代表可以正常写入  这里直接更新了偏移量但是数据还没写入 推测是将它作为一个 event 存放到队列中
            if (firstLogEntry.getId().getIndex() == this.lastLogIndex + 1) {
                // fast path
                this.lastLogIndex = lastLogEntry.getId().getIndex();
            } else {
                // Appending entries overlap the local ones. We should find if there
                // is a conflicting index from which we should truncate the local
                // ones.
                // 代表有部分数据是重复的
                int conflictingIndex = 0;
                for (; conflictingIndex < entries.size(); conflictingIndex++) {
                    // 这里先检查 任期是在哪里出现了出入  因为数据发生重复 很可能是在某个时期出现了脑裂 导致收到不同任期相同下标的数据
                    // conflictingIndex 代表发现冲突的起点  而在0～conflictingindex 之间的数据都是重复数据
                    if (unsafeGetTerm(entries.get(conflictingIndex).getId().getIndex()) != entries  //
                        .get(conflictingIndex).getId().getTerm()) {
                        break;
                    }
                }
                // 该index在 LogStorage 查找到的 LogEntry 的任期 与当前要写入的 list 中某个entey 对应的任期不一致
                // 如果 conflictingIndex == entries.size() 代表 遍历完了所有数据且没有出现冲突
                if (conflictingIndex != entries.size()) {
                    // 代表需要截取部分 Logstorage 的数据 那些数据是因为脑裂导致旧的leader 还能写入的 无效数据
                    if (entries.get(conflictingIndex).getId().getIndex() <= this.lastLogIndex) {
                        // Truncate all the conflicting entries to make local logs
                        // consensus with the leader.
                        unsafeTruncateSuffix(entries.get(conflictingIndex).getId().getIndex() - 1);
                    }
                    this.lastLogIndex = lastLogEntry.getId().getIndex();
                } // else this is a duplicated AppendEntriesRequest, we have
                  // nothing to do besides releasing all the entries
                if (conflictingIndex > 0) {
                    // Remove duplication
                    entries.subList(0, conflictingIndex).clear();
                }
            }
            return true;
        }
    }

    /**
     * 获取对应的配置实体
     * @param index
     * @return
     */
    @Override
    public ConfigurationEntry getConfiguration(final long index) {
        this.readLock.lock();
        try {
            return this.configManager.get(index);
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 检验当前conf 与 保存的最后一个 配置是否相同
     * 一般调用该方法的时机是 从快照文件拉取数据后  看来快照的数据优先级会比 LogManager本身收到的写入数据高 是因为快照必然是准确的数据么
     * @param current
     * @return
     */
    @Override
    public ConfigurationEntry checkAndSetConfiguration(final ConfigurationEntry current) {
        if (current == null) {
            return null;
        }
        this.readLock.lock();
        try {
            final ConfigurationEntry lastConf = this.configManager.getLastConfiguration();
            if (lastConf != null && !lastConf.isEmpty() && !current.getId().equals(lastConf.getId())) {
                return lastConf;
            }
        } finally {
            this.readLock.unlock();
        }
        return current;
    }

    /**
     * 生成 检测新log 写入的回调对象
     * @param expectedLastLogIndex  expected last index of log
     * @param cb                    callback
     * @param arg                   the waiter pass-in argument
     * @return
     */
    @Override
    public long wait(final long expectedLastLogIndex, final NewLogCallback cb, final Object arg) {
        final WaitMeta wm = new WaitMeta(cb, arg, 0);
        return notifyOnNewLog(expectedLastLogIndex, wm);
    }

    /**
     * 将回调对象设置到map中
     * @param expectedLastLogIndex  代表预期触发的下标 如果当前不是该下标 直接触发回调
     * @param wm
     * @return
     */
    private long notifyOnNewLog(final long expectedLastLogIndex, final WaitMeta wm) {
        this.writeLock.lock();
        try {
            // index 不匹配时 直接执行 如果匹配 先存放到 waitMap 中 在某个合适的时机执行
            if (expectedLastLogIndex != this.lastLogIndex || this.stopped) {
                wm.errorCode = this.stopped ? RaftError.ESTOP.getNumber() : 0;
                Utils.runInThread(() -> runOnNewLog(wm));
                return 0L;
            }
            if (this.nextWaitId == 0) { //skip 0
                ++this.nextWaitId;
            }
            final long waitId = this.nextWaitId++;
            this.waitMap.put(waitId, wm);
            return waitId;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean removeWaiter(final long id) {
        this.writeLock.lock();
        try {
            return this.waitMap.remove(id) != null;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 设置最后生效的 index 和 任期
     * @param appliedId
     */
    @Override
    public void setAppliedId(final LogId appliedId) {
        LogId clearId;
        this.writeLock.lock();
        try {
            if (appliedId.compareTo(this.appliedId) < 0) {
                return;
            }
            this.appliedId = appliedId.copy();
            // 代表要从哪里开始清除  一开始数据是写入到内存中的  之后在某个时间点 进行刷盘 这里选择较小的偏移量 将 内存中这部分数据移除掉
            clearId = this.diskId.compareTo(this.appliedId) <= 0 ? this.diskId : this.appliedId;
        } finally {
            this.writeLock.unlock();
        }
        if (clearId != null) {
            // 从内存中移除数据
            clearMemoryLogs(clearId);
        }
    }

    void runOnNewLog(final WaitMeta wm) {
        wm.onNewLog.onNewLog(wm.arg, wm.errorCode);
    }

    /**
     * 检查一致性
     * @return
     */
    @Override
    public Status checkConsistency() {
        this.readLock.lock();
        try {
            Requires.requireTrue(this.firstLogIndex > 0);
            Requires.requireTrue(this.lastLogIndex >= 0);
            // 如果快照是0 firstLogIndex必须是1  firstLogIndex 可能就是 快照的尾部
            if (this.lastSnapshotId.equals(new LogId(0, 0))) {
                if (this.firstLogIndex == 1) {
                    return Status.OK();
                }
                return new Status(RaftError.EIO, "Missing logs in (0, %d)", this.firstLogIndex);
            } else {
                if (this.lastSnapshotId.getIndex() >= this.firstLogIndex - 1
                    && this.lastSnapshotId.getIndex() <= this.lastLogIndex) {
                    return Status.OK();
                }
                return new Status(RaftError.EIO, "There's a gap between snapshot={%d, %d} and log=[%d, %d] ",
                    this.lastSnapshotId.toString(), this.lastSnapshotId.getTerm(), this.firstLogIndex,
                    this.lastLogIndex);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final long _firstLogIndex;
        final long _lastLogIndex;
        final String _diskId;
        final String _appliedId;
        final String _lastSnapshotId;
        this.readLock.lock();
        try {
            _firstLogIndex = this.firstLogIndex;
            _lastLogIndex = this.lastLogIndex;
            _diskId = String.valueOf(this.diskId);
            _appliedId = String.valueOf(this.appliedId);
            _lastSnapshotId = String.valueOf(this.lastSnapshotId);
        } finally {
            this.readLock.unlock();
        }
        out.print("  storage: [") //
            .print(_firstLogIndex) //
            .print(", ") //
            .print(_lastLogIndex) //
            .println(']');
        out.print("  diskId: ") //
            .println(_diskId);
        out.print("  appliedId: ") //
            .println(_appliedId);
        out.print("  lastSnapshotId: ") //
            .println(_lastSnapshotId);
    }
}
