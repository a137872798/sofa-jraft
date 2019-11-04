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

import java.util.concurrent.locks.Lock;

import org.rocksdb.RocksIterator;

import com.alipay.sofa.jraft.rhea.errors.InvalidIteratorVersion;

/**
 * 在RawKVStore api 中暴露给用户的迭代器方法  使用后需要关闭
 * @author jiachun.fjc
 */
public class RocksKVIterator implements KVIterator {

    /**
     * 关联的db 存储
     */
    private final RocksRawKVStore rocksRawKVStore;
    /**
     * rocksDB 本身就具备将数据以迭代器方式进行输出的能力
     */
    private final RocksIterator   it;
    /**
     * 读锁
     */
    private final Lock            dbReadLock;
    private final long            dbVersion;

    public RocksKVIterator(RocksRawKVStore rocksRawKVStore, RocksIterator it, Lock dbReadLock, long dbVersion) {
        this.rocksRawKVStore = rocksRawKVStore;
        this.it = it;
        this.dbReadLock = dbReadLock;
        this.dbVersion = dbVersion;
    }

    /**
     * 判断是否还有数据
     * @return
     */
    @Override
    public boolean isValid() {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            // 校验 db.version 是否一致
            ensureSafety();
            return it.isValid();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 返回第一个数据
     */
    @Override
    public void seekToFirst() {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            it.seekToFirst();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void seekToLast() {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            it.seekToLast();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 通过 byte[] 去寻找value   推测该方法会修改迭代器内部的指针 之后配合next 就能获取到需要的value
     * @param target byte array describing a key or a
     */
    @Override
    public void seek(final byte[] target) {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            it.seek(target);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 将迭代器定位到指定 数据的前一个
     * @param target byte array describing a key or a
     */
    @Override
    public void seekForPrev(final byte[] target) {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            it.seekForPrev(target);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void next() {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            it.next();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void prev() {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            it.prev();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 返回当前指向数据的 key
     * @return
     */
    @Override
    public byte[] key() {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            return it.key();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] value() {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            return it.value();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 通过调用该方法 关闭 rocksDB 的迭代器
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        final Lock readLock = this.dbReadLock;
        readLock.lock();
        try {
            ensureSafety();
            it.close();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 确认数据是否正常
     */
    private void ensureSafety() {
        if (this.dbVersion != this.rocksRawKVStore.getDatabaseVersion()) {
            throw new InvalidIteratorVersion("current iterator is belong to the older version of db: " + this.dbVersion
                                             + ", the newest db version: " + this.rocksRawKVStore.getDatabaseVersion());
        }
    }
}
