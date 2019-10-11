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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksObject;
import org.rocksdb.util.SizeUnit;

/**
 *
 * @author jiachun.fjc
 */
public final class StorageOptionsFactory {

    private static final Map<String, DBOptions>           rocksDBOptionsTable      = new ConcurrentHashMap<>();
    private static final Map<String, ColumnFamilyOptions> columnFamilyOptionsTable = new ConcurrentHashMap<>();

    /**
     * Releases all storage options from the responsibility of freeing the
     * underlying native C++ object.
     *
     * Note, that once an instance of options has been released, calling any
     * of its functions will lead to undefined behavior.
     */
    public static void releaseAllOptions() {
        for (final DBOptions opts : rocksDBOptionsTable.values()) {
            if (opts != null) {
                opts.close();
            }
        }
        for (final ColumnFamilyOptions opts : columnFamilyOptionsTable.values()) {
            if (opts != null) {
                opts.close();
            }
        }
    }

    /**
     * Users can register a custom rocksdb dboptions, then the related
     * classes will get their options by the key of their own class
     * name.  If the user does not register an options, a default options
     * will be provided.
     *
     * @param cls  the key of DBOptions
     * @param opts the DBOptions
     */
    public static void registerRocksDBOptions(final Class<?> cls, final DBOptions opts) {
        Requires.requireNonNull(cls, "cls");
        Requires.requireNonNull(opts, "opts");
        if (rocksDBOptionsTable.putIfAbsent(cls.getName(), opts) != null) {
            throw new IllegalStateException("DBOptions with class key [" + cls.getName()
                                            + "] has already been registered");
        }
    }

    /**
     * Get a new default DBOptions or a copy of the exist DBOptions.
     * Users should call DBOptions#close() to release resources themselves.
     * 获取db 相关配置
     * @param cls the key of DBOptions
     * @return new default DBOptions or a copy of the exist DBOptions
     */
    public static DBOptions getRocksDBOptions(final Class<?> cls) {
        Requires.requireNonNull(cls, "cls");
        // 根据key 获取对应的 db选项 默认的name 为 RocksDBLogStorage全限定名
        DBOptions opts = rocksDBOptionsTable.get(cls.getName());
        if (opts == null) {
            // 生成默认配置
            final DBOptions newOpts = getDefaultRocksDBOptions();
            opts = rocksDBOptionsTable.putIfAbsent(cls.getName(), newOpts);
            if (opts == null) {
                opts = newOpts;
            } else {
                newOpts.close();
            }
        }
        // NOTE: This does a shallow copy, which means env, rate_limiter,
        // sst_file_manager, info_log and other pointers will be cloned!
        return new DBOptions(checkInvalid(opts));
    }

    /**
     * 获取默认的 db 配置  这里使用的是 rocksDB 官方推荐的默认配置
     * @return
     */
    public static DBOptions getDefaultRocksDBOptions() {
        // Turn based on https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        final DBOptions opts = new DBOptions();

        // If this value is set to true, then the database will be created if it is
        // missing during {@code RocksDB.open()}.
        opts.setCreateIfMissing(true);

        // If true, missing column families will be automatically created.
        opts.setCreateMissingColumnFamilies(true);

        // Number of open files that can be used by the DB.  You may need to increase
        // this if your database has a large working set. Value -1 means files opened
        // are always kept open.
        opts.setMaxOpenFiles(-1);

        // The maximum number of concurrent background compactions. The default is 1,
        // but to fully utilize your CPU and storage you might want to increase this
        // to approximately number of cores in the system.
        opts.setMaxBackgroundCompactions(Math.min(Utils.cpus(), 4));

        // The maximum number of concurrent flush operations. It is usually good enough
        // to set this to 1.
        opts.setMaxBackgroundFlushes(1);

        return opts;
    }

    /**
     * Users can register a custom rocksdb column family options, then the
     * related classes will get their options by the key of their own class
     * name.  If the user does not register an options, a default options
     * will be provided.
     *
     * @param cls  the key of ColumnFamilyOptions
     * @param opts the ColumnFamilyOptions
     */
    public static void registerRocksDBColumnFamilyOptions(final Class<?> cls, final ColumnFamilyOptions opts) {
        Requires.requireNonNull(cls, "cls");
        Requires.requireNonNull(opts, "opts");
        if (columnFamilyOptionsTable.putIfAbsent(cls.getName(), opts) != null) {
            throw new IllegalStateException("ColumnFamilyOptions with class key [" + cls.getName()
                                            + "] has already been registered");
        }
    }

    /**
     * Get a new default ColumnFamilyOptions or a copy of the exist
     * ColumnFamilyOptions.  Users should call ColumnFamilyOptions#close()
     * to release resources themselves.
     *
     * @param cls the key of ColumnFamilyOptions
     * @return new default ColumnFamilyOptions or a copy of the exist
     * ColumnFamilyOptions
     * 获取列族配置
     */
    public static ColumnFamilyOptions getRocksDBColumnFamilyOptions(final Class<?> cls) {
        Requires.requireNonNull(cls, "cls");
        ColumnFamilyOptions opts = columnFamilyOptionsTable.get(cls.getName());
        if (opts == null) {
            final ColumnFamilyOptions newOpts = getDefaultRocksDBColumnFamilyOptions();
            opts = columnFamilyOptionsTable.putIfAbsent(cls.getName(), newOpts);
            if (opts == null) {
                opts = newOpts;
            } else {
                newOpts.close();
            }
        }
        // NOTE: This does a shallow copy, which means comparator, merge_operator,
        // compaction_filter, compaction_filter_factory and other pointers will be
        // cloned!
        return new ColumnFamilyOptions(checkInvalid(opts));
    }

    /**
     * 生成列族配置
     * @return
     */
    public static ColumnFamilyOptions getDefaultRocksDBColumnFamilyOptions() {
        final ColumnFamilyOptions opts = new ColumnFamilyOptions();

        // Flushing options:
        // write_buffer_size sets the size of a single mem_table. Once mem_table exceeds
        // this size, it is marked immutable and a new one is created.
        opts.setWriteBufferSize(64 * SizeUnit.MB);

        // Flushing options:
        // max_write_buffer_number sets the maximum number of mem_tables, both active
        // and immutable.  If the active mem_table fills up and the total number of
        // mem_tables is larger than max_write_buffer_number we stall further writes.
        // This may happen if the flush process is slower than the write rate.
        opts.setMaxWriteBufferNumber(3);

        // Flushing options:
        // min_write_buffer_number_to_merge is the minimum number of mem_tables to be
        // merged before flushing to storage. For example, if this option is set to 2,
        // immutable mem_tables are only flushed when there are two of them - a single
        // immutable mem_table will never be flushed.  If multiple mem_tables are merged
        // together, less data may be written to storage since two updates are merged to
        // a single key. However, every Get() must traverse all immutable mem_tables
        // linearly to check if the key is there. Setting this option too high may hurt
        // read performance.
        opts.setMinWriteBufferNumberToMerge(1);

        // Level Style Compaction:
        // level0_file_num_compaction_trigger -- Once level 0 reaches this number of
        // files, L0->L1 compaction is triggered. We can therefore estimate level 0
        // size in stable state as
        // write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger.
        opts.setLevel0FileNumCompactionTrigger(10);

        // Soft limit on number of level-0 files. We start slowing down writes at this
        // point. A value 0 means that no writing slow down will be triggered by number
        // of files in level-0.
        opts.setLevel0SlowdownWritesTrigger(20);

        // Maximum number of level-0 files.  We stop writes at this point.
        opts.setLevel0StopWritesTrigger(40);

        // Level Style Compaction:
        // max_bytes_for_level_base and max_bytes_for_level_multiplier
        //  -- max_bytes_for_level_base is total size of level 1. As mentioned, we
        // recommend that this be around the size of level 0. Each subsequent level
        // is max_bytes_for_level_multiplier larger than previous one. The default
        // is 10 and we do not recommend changing that.
        opts.setMaxBytesForLevelBase(512 * SizeUnit.MB);

        // Level Style Compaction:
        // target_file_size_base and target_file_size_multiplier
        //  -- Files in level 1 will have target_file_size_base bytes. Each next
        // level's file size will be target_file_size_multiplier bigger than previous
        // one. However, by default target_file_size_multiplier is 1, so files in all
        // L1..LMax levels are equal. Increasing target_file_size_base will reduce total
        // number of database files, which is generally a good thing. We recommend setting
        // target_file_size_base to be max_bytes_for_level_base / 10, so that there are
        // 10 files in level 1.
        opts.setTargetFileSizeBase(64 * SizeUnit.MB);

        // If prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
        // create prefix bloom for memtable with the size of
        // write_buffer_size * memtable_prefix_bloom_size_ratio.
        // If it is larger than 0.25, it is santinized to 0.25.
        opts.setMemtablePrefixBloomSizeRatio(0.125);

        // Seems like the rocksDB jni for Windows doesn't come linked with any of the
        // compression type
        if (!Platform.isWindows()) {
            opts.setCompressionType(CompressionType.LZ4_COMPRESSION) //
                .setCompactionStyle(CompactionStyle.LEVEL) //
                .optimizeLevelStyleCompaction();
        }

        return opts;
    }

    private static <T extends RocksObject> T checkInvalid(final T opts) {
        if (!opts.isOwningHandle()) {
            throw new IllegalStateException(
                "the instance of options [" + opts
                        + "] has been released, calling any of its functions will lead to undefined behavior.");
        }
        return opts;
    }

    private StorageOptionsFactory() {
    }
}
