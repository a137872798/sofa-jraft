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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Log storage based on rocksdb.
 * 日志存储的默认实现    rocksdb 可以看作一个简单的 key-value 型存储数据库  具体实现先不看
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-06 7:27:47 AM
 */
public class RocksDBLogStorage implements LogStorage {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogStorage.class);

    static {
        // 推测是加载相关配置 不细看
        RocksDB.loadLibrary();
    }

    /**
     * Write batch template.
     * 批量写入模板
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2017-Nov-08 11:19:22 AM
     */
    private interface WriteBatchTemplate {

        /**
         * 执行批量写入任务
         * @param batch
         * @throws RocksDBException
         * @throws IOException
         */
        void execute(WriteBatch batch) throws RocksDBException, IOException;
    }

    /**
     * 存储路径
     */
    private final String                    path;
    /**
     * 是否同步写入
     */
    private final boolean                   sync;
    /**
     * rocksdb 存储对象
     */
    private RocksDB                         db;
    /**
     * 相关配置
     */
    private DBOptions                       dbOptions;
    /**
     * 写入配置
     */
    private WriteOptions                    writeOptions;
    /**
     * 好像是 有关于列的 配置
     */
    private final List<ColumnFamilyOptions> cfOptions     = new ArrayList<>();
    private ColumnFamilyHandle              defaultHandle;
    private ColumnFamilyHandle              confHandle;
    private ReadOptions                     totalOrderReadOptions;
    private final ReadWriteLock             readWriteLock = new ReentrantReadWriteLock();
    private final Lock                      readLock      = this.readWriteLock.readLock();
    private final Lock                      writeLock     = this.readWriteLock.writeLock();

    private volatile long                   firstLogIndex = 1;

    private volatile boolean                hasLoadFirstLogIndex;

    // 在写入 和 读取时会做编解码的操作
    private LogEntryEncoder                 logEntryEncoder;
    private LogEntryDecoder                 logEntryDecoder;

    public RocksDBLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        // 获取到是同步写入还是异步写入
        this.sync = raftOptions.isSync();
    }

    /**
     * 创建表配置
     * @return
     */
    private static BlockBasedTableConfig createTableConfig() {
        return new BlockBasedTableConfig() //
            // Begin to use partitioned index filters
            // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters#how-to-use-it
            .setIndexType(IndexType.kTwoLevelIndexSearch) //
            .setFilter(new BloomFilter(16, false)) //
            .setPartitionFilters(true) //
            .setMetadataBlockSize(8 * SizeUnit.KB) //
            .setCacheIndexAndFilterBlocks(false) //
            .setCacheIndexAndFilterBlocksWithHighPriority(true) //
            .setPinL0FilterAndIndexBlocksInCache(true) //
            // End of partitioned index filters settings.
            .setBlockSize(4 * SizeUnit.KB)//
            .setBlockCacheSize(512 * SizeUnit.MB) //
            .setCacheNumShardBits(8);
    }

    /**
     * 生成相关配置
     * @return
     */
    public static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RocksDBLogStorage.class);
    }

    /**
     * 创建列族配置
     * @return
     */
    public static ColumnFamilyOptions createColumnFamilyOptions() {
        // 创建表配置
        final BlockBasedTableConfig tConfig = createTableConfig();
        // 获取列族配置 应该也是使用官方推荐的配置
        return StorageOptionsFactory.getRocksDBColumnFamilyOptions(RocksDBLogStorage.class) //
            .useFixedLengthPrefixExtractor(8) //
            .setTableFormatConfig(tConfig) //
            .setMergeOperator(new StringAppendOperator());
    }

    /**
     * 进行初始化 操作
     * @param opts  使用的配置对象
     * @return
     */
    @Override
    public boolean init(final LogStorageOptions opts) {
        // 看来必须要存在编解码器 和 配置工厂才能正常初始化
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        // 串行
        this.writeLock.lock();
        try {
            // 代表已经完成初始化
            if (this.db != null) {
                LOG.warn("RocksDBLogStorage init() already.");
                return true;
            }
            // 获取 编解码器
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");
            // 生成db 相关的配置
            this.dbOptions = createDBOptions();

            // 设置写相关属性
            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(this.sync);
            // 读配置
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);

            // 将新配置写入到 configManger中
            return initAndLoad(opts.getConfigurationManager());
        } catch (final RocksDBException e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }

    }

    /**
     * 加载配置工厂中的配置
     * @param confManager
     * @return
     * @throws RocksDBException
     */
    private boolean initAndLoad(final ConfigurationManager confManager) throws RocksDBException {
        // 代表还没有载入数据
        this.hasLoadFirstLogIndex = false;
        // 首个日志文件下标
        this.firstLogIndex = 1;
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        // 创建列族配置
        final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
        this.cfOptions.add(cfOption);
        // Column family to store configuration log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), cfOption));
        // Default column family to store user data log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));

        // 打开数据库连接
        openDB(columnFamilyDescriptors);
        // 读取当前的数据 如果发现新配置 存储到configManger 中 如果发现了 firstLogIndex相关的 就更新偏移量
        load(confManager);
        // 默认返回true 该方法应该是代表是否加载成功
        return onInitLoaded();
    }

    /**
     * First log index and last log index key in configuration column family.
     */
    public static final byte[] FIRST_LOG_IDX_KEY = Utils.getBytes("meta/firstLogIndex");

    /**
     * 从 config 中加载配置
     * @param confManager
     */
    private void load(final ConfigurationManager confManager) {
        // 确保 db 已经初始化
        checkState();
        // 生成 db 迭代器对象
        try (final RocksIterator it = this.db.newIterator(this.confHandle, this.totalOrderReadOptions)) {
            it.seekToFirst();
            while (it.isValid()) {
                // rocksDB 本身是一款kv 数据库
                final byte[] ks = it.key();
                final byte[] bs = it.value();

                // LogEntry index   如果key 的长度是8 代表就是 LogEntry
                if (ks.length == 8) {
                    // 使用解码器 对value 进行解码
                    final LogEntry entry = this.logEntryDecoder.decode(bs);
                    if (entry != null) {
                        // 如果是配置信息
                        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                            final ConfigurationEntry confEntry = new ConfigurationEntry();
                            confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                            confEntry.setConf(new Configuration(entry.getPeers()));
                            if (entry.getOldPeers() != null) {
                                confEntry.setOldConf(new Configuration(entry.getOldPeers()));
                            }
                            // 这里将 变更配置加载到 配置管理器
                            if (confManager != null) {
                                confManager.add(confEntry);
                            }
                        }
                    } else {
                        LOG.warn("Fail to decode conf entry at index {}, the log data is: {}.", Bits.getLong(ks, 0),
                            BytesUtil.toHex(bs));
                    }
                } else {
                    // 如果是代表 firstLogIndex 的元数据
                    if (Arrays.equals(FIRST_LOG_IDX_KEY, ks)) {
                        // 更新firstLogIndex
                        setFirstLogIndex(Bits.getLong(bs, 0));
                        // 丢弃之前的数据
                        truncatePrefixInBackground(0L, this.firstLogIndex);
                    } else {
                        LOG.warn("Unknown entry in configuration storage key={}, value={}.", BytesUtil.toHex(ks),
                            BytesUtil.toHex(bs));
                    }
                }
                it.next();
            }
        }
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    /**
     * Save the first log index into conf column family.
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            checkState();
            this.db.put(this.confHandle, this.writeOptions, FIRST_LOG_IDX_KEY, vs);
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to save first log index {}.", firstLogIndex, e);
            return false;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 打开数据库连接
     * @param columnFamilyDescriptors 传入 列族描述信息
     * @throws RocksDBException
     */
    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        // 根据初始化时传入的 path 创建文件
        final File dir = new File(this.path);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }
        // 基于特定路径打开db
        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);

        assert (columnFamilyHandles.size() == 2);
        this.confHandle = columnFamilyHandles.get(0);
        this.defaultHandle = columnFamilyHandles.get(1);
    }

    private void checkState() {
        Requires.requireNonNull(this.db, "DB not initialized or destroyed");
    }

    /**
     * Execute write batch template.
     * 批量写入
     * @param template write batch template
     */
    private boolean executeBatch(final WriteBatchTemplate template) {
        this.readLock.lock();
        if (this.db == null) {
            LOG.warn("DB not initialized or destroyed.");
            this.readLock.unlock();
            return false;
        }
        // 创建一个批量写入任务 并使用模板执行
        try (final WriteBatch batch = new WriteBatch()) {
            template.execute(batch);
            this.db.write(this.writeOptions, batch);
        } catch (final RocksDBException e) {
            LOG.error("Execute batch failed with rocksdb exception.", e);
            return false;
        } catch (final IOException e) {
            LOG.error("Execute batch failed with io exception.", e);
            return false;
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    /**
     * 对应LifeCycle 中的方法 停止整个日志存储工具
     */
    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            // The shutdown order is matter.
            // 1. close column family handles
            // 关闭列族处理器 和 db
            closeDB();
            // 钩子
            onShutdown();
            // 2. close column family options.  关闭列族配置
            for (final ColumnFamilyOptions opt : this.cfOptions) {
                opt.close();
            }
            // 3. close options  关闭读写选项
            this.writeOptions.close();
            this.totalOrderReadOptions.close();
            // 4. help gc.  清空引用
            this.cfOptions.clear();
            this.db = null;
            this.totalOrderReadOptions = null;
            this.writeOptions = null;
            this.defaultHandle = null;
            this.confHandle = null;
            LOG.info("DB destroyed, the db path is: {}.", this.path);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void closeDB() {
        this.confHandle.close();
        this.defaultHandle.close();
        this.db.close();
    }

    /**
     * 获取首个日志文件对应的偏移量
     * @return
     */
    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        RocksIterator it = null;
        try {
            // 如果从元数据中获取到了 就直接返回
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();
            // 返回迭代器对象
            it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions);
            // 定位到第一个 LogEntry
            it.seekToFirst();
            // 确保该对象非空
            if (it.isValid()) {
                // 获取偏移量
                final long ret = Bits.getLong(it.key(), 0);
                saveFirstLogIndex(ret);
                setFirstLogIndex(ret);
                return ret;
            }
            return 1L;
        } finally {
            if (it != null) {
                it.close();
            }
            this.readLock.unlock();
        }
    }

    /**
     * 获取最后一个 logEntry 的偏移量 基本与firstLogIndex 方法相同
     * @return
     */
    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        checkState();
        try (final RocksIterator it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions)) {
            // 区别就是这个方法 能直接定位到最后一个LogEntry
            it.seekToLast();
            if (it.isValid()) {
                return Bits.getLong(it.key(), 0);
            }
            return 0L;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 通过偏移量直接定位到某个 LogEntry
     * @param index
     * @return
     */
    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            // 如果小于当前保存的 index 就不再获取
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            // 将index 封装成 key
            final byte[] keyBytes = getKeyBytes(index);
            // 通过key 查询value 并转化为bytes
            final byte[] bs = onDataGet(index, getValueFromRocksDB(keyBytes));
            if (bs != null) {
                // 反序列化成 LogEntry
                final LogEntry entry = this.logEntryDecoder.decode(bs);
                if (entry != null) {
                    return entry;
                } else {
                    LOG.error("Bad log entry format for index={}, the log data is: {}.", index, BytesUtil.toHex(bs));
                    // invalid data remove? TODO
                    return null;
                }
            }
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to get log entry at index {}.", index, e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    protected byte[] getValueFromRocksDB(final byte[] keyBytes) throws RocksDBException {
        checkState();
        return this.db.get(this.defaultHandle, keyBytes);
    }

    protected byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    /**
     * 获取 生成对应日志的 节点任期
     * @param index
     * @return
     */
    @Override
    public long getTerm(final long index) {
        // entry 内部就存储了任期信息
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    /**
     * 批量写入配置文件   推测 WriteBatch 并不会 立即写入到 db 中 而是在某个合适的时机写入 当然它会保证写入的数据一定能被及时读取到
     * @param entry
     * @param batch
     * @throws RocksDBException
     */
    private void addConfBatch(final LogEntry entry, final WriteBatch batch) throws RocksDBException {
        // 以index 作为key
        final byte[] ks = getKeyBytes(entry.getId().getIndex());
        final byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.defaultHandle, ks, content);
        batch.put(this.confHandle, ks, content);
    }

    private void addDataBatch(final LogEntry entry, final WriteBatch batch) throws RocksDBException, IOException {
        final long logIndex = entry.getId().getIndex();
        final byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.defaultHandle, getKeyBytes(logIndex), onDataAppend(logIndex, content));
    }

    /**
     * 保存某个 日志实体到 LogStorage中
     * @param entry
     * @return
     */
    @Override
    public boolean appendEntry(final LogEntry entry) {
        // 在executeBatch 中还是加锁了 唯一的区别是使用了一个WriteBatch 对象写入的 该对象也是 rocksDB 内部自带的类
        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
            return executeBatch(batch -> addConfBatch(entry, batch));
        } else {
            this.readLock.lock();
            try {
                if (this.db == null) {
                    LOG.warn("DB not initialized or destroyed.");
                    return false;
                }
                final long logIndex = entry.getId().getIndex();
                final byte[] valueBytes = this.logEntryEncoder.encode(entry);
                // 一个钩子 默认返回valueBytes
                final byte[] newValueBytes = onDataAppend(logIndex, valueBytes);
                this.db.put(this.defaultHandle, this.writeOptions, getKeyBytes(logIndex), newValueBytes);
                if (newValueBytes != valueBytes) {
                    doSync();
                }
                return true;
            } catch (final RocksDBException | IOException e) {
                LOG.error("Fail to append entry.", e);
                return false;
            } finally {
                this.readLock.unlock();
            }
        }
    }

    private void doSync() throws IOException {
        if (this.sync) {
            onSync();
        }
    }

    /**
     * 批量写入数据
     * @param entries
     * @return
     */
    @Override
    public int appendEntries(final List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final boolean ret = executeBatch(batch -> {
            for (int i = 0; i < entriesCount; i++) {
                final LogEntry entry = entries.get(i);
                // 就是利用 batch 对象存储数据
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    addConfBatch(entry, batch);
                } else {
                    addDataBatch(entry, batch);
                }
            }
            // 默认情况下 是空实现 由子类拓展
            doSync();
        });

        if (ret) {
            return entriesCount;
        } else {
            return 0;
        }
    }

    /**
     * 剔除指定偏移量前的数据
     * @param firstIndexKept
     * @return
     */
    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.readLock.lock();
        try {
            final long startIndex = getFirstLogIndex();
            // 将新的偏移量保存到 db中
            final boolean ret = saveFirstLogIndex(firstIndexKept);
            if (ret) {
                // 更新 first 偏移量
                setFirstLogIndex(firstIndexKept);
            }
            // 删除中间的数据
            truncatePrefixInBackground(startIndex, firstIndexKept);
            return ret;
        } finally {
            this.readLock.unlock();
        }

    }

    /**
     * 丢弃给定的 2个index 之间的数据
     * @param startIndex
     * @param firstIndexKept
     */
    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.  异步删除日志
        Utils.runInThread(() -> {
            // 也就是 允许读取
            this.readLock.lock();
            try {
                if (this.db == null) {
                    return;
                }
                // 丢弃指定数据  钩子函数
                onTruncatePrefix(startIndex, firstIndexKept);
                // 使用handle 进行操作 这里是 RocksDB 相关的api 先不看
                this.db.deleteRange(this.defaultHandle, getKeyBytes(startIndex), getKeyBytes(firstIndexKept));
                this.db.deleteRange(this.confHandle, getKeyBytes(startIndex), getKeyBytes(firstIndexKept));
            } catch (final RocksDBException | IOException e) {
                LOG.error("Fail to truncatePrefix {}.", firstIndexKept, e);
            } finally {
                this.readLock.unlock();
            }
        });
    }

    /**
     * 删除后面的数据 必须阻塞完成 不能在后台线程处理
     * @param lastIndexKept
     * @return
     */
    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.readLock.lock();
        try {
            try {
                onTruncateSuffix(lastIndexKept);
            } finally {
                this.db.deleteRange(this.defaultHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                    getKeyBytes(getLastLogIndex() + 1));
                this.db.deleteRange(this.confHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                    getKeyBytes(getLastLogIndex() + 1));
            }
            return true;
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to truncateSuffix {}.", lastIndexKept, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    /**
     * 重置偏移量
     * @param nextLogIndex
     * @return
     */
    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try (final Options opt = new Options()) {
            // 获取偏移量对应的 entry
            LogEntry entry = getEntry(nextLogIndex);
            // 关闭db
            closeDB();
            try {
                RocksDB.destroyDB(this.path, opt);
                // 钩子 noop
                onReset(nextLogIndex);
                // 迭代 db 中所有数据 重新获取 firstLogIndex (由于没有传入 confManager 所以不会保存新配置)
                if (initAndLoad(null)) {
                    if (entry == null) {
                        entry = new LogEntry();
                        entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                        entry.setId(new LogId(nextLogIndex, 0));
                        LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
                    }
                    // 将之前的 entry 重新上传到db 中
                    return appendEntry(entry);
                } else {
                    return false;
                }
            } catch (final RocksDBException e) {
                LOG.error("Fail to reset next log index.", e);
                return false;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    // Hooks for {@link RocksDBSegmentLogStorage}

    /**
     * Called after opening RocksDB and loading configuration into conf manager.
     */
    protected boolean onInitLoaded() {
        return true;
    }

    /**
     * Called after closing db.
     */
    protected void onShutdown() {
    }

    /**
     * Called after resetting db.
     *
     * @param nextLogIndex next log index
     */
    protected void onReset(final long nextLogIndex) {
    }

    /**
     * Called after truncating prefix logs in rocksdb.
     *
     * @param startIndex     the start index
     * @param firstIndexKept the first index to kept
     */
    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) throws RocksDBException,
                                                                                     IOException {
    }

    /**
     * Called when sync data into file system.
     */
    protected void onSync() throws IOException {
    }

    /**
     * Called after truncating suffix logs in rocksdb.
     *
     * @param lastIndexKept the last index to kept
     */
    protected void onTruncateSuffix(final long lastIndexKept) throws RocksDBException, IOException {
    }

    /**
     * Called before appending data entry.
     *
     * @param logIndex the log index
     * @param value    the data value in log entry.
     * @return the new value
     */
    protected byte[] onDataAppend(final long logIndex, final byte[] value) throws IOException {
        return value;
    }

    /**
     * Called after getting data from rocksdb.
     *
     * @param logIndex the log index
     * @param value    the value in rocksdb
     * @return the new value
     */
    protected byte[] onDataGet(final long logIndex, final byte[] value) throws IOException {
        return value;
    }
}
