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

import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.BootstrapOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;

/**
 * Some helper methods for jraft usage.
 * jraft 工具类 可以直接通过 字符串构建 对应的 PeerId/Configuration 类等
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-23 3:48:45 PM
 */
public final class JRaftUtils {

    private JRaftUtils() {
    }

    /**
     * Bootstrap a non-empty raft node.
     * 根据配置信息生成一个 node 对象
     * @param opts options of bootstrap 启动选项
     * @return true if bootstrap success 是否启动成功
     */
    public static boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        final NodeImpl node = new NodeImpl();
        final boolean ret = node.bootstrap(opts);
        node.shutdown();
        node.join();
        return ret;
    }

    /**
     * Create a executor with size.
     *
     * @param prefix thread name prefix
     * @param number thread number
     * @return a new {@link ThreadPoolExecutor} instance
     */
    public static Executor createExecutor(final String prefix, final int number) {
        if (number <= 0) {
            return null;
        }
        return ThreadPoolUtil.newBuilder() //
            .poolName(prefix) //
            .enableMetric(true) //
            .coreThreads(number) //
            .maximumThreads(number) //
            .keepAliveSeconds(60L) //
            .workQueue(new SynchronousQueue<>()) //
            .threadFactory(createThreadFactory(prefix)) //
            .build();
    }

    /**
     * Create a thread factory.
     *
     * @param prefixName the prefix name of thread
     * @return a new {@link ThreadFactory} instance
     *
     * @since 0.0.3
     */
    public static ThreadFactory createThreadFactory(final String prefixName) {
        return new NamedThreadFactory(prefixName, true);
    }

    /**
     * Create a configuration from a string in the form of "host1:port1[:idx],host2:port2[:idx]......",
     * returns a empty configuration when string is blank.
     * 通过一个字符串直接生成conf对象
     */
    public static Configuration getConfiguration(final String s) {
        final Configuration conf = new Configuration();
        if (StringUtils.isBlank(s)) {
            return conf;
        }
        if (conf.parse(s)) {
            return conf;
        }
        throw new IllegalArgumentException("Invalid conf str:" + s);
    }

    /**
     * Create a peer from a string in the form of "host:port[:idx]",
     * returns a empty peer when string is blank.
     * 通过字符串 直接生成 peerId 对象
     */
    public static PeerId getPeerId(final String s) {
        final PeerId peer = new PeerId();
        if (StringUtils.isBlank(s)) {
            return peer;
        }
        if (peer.parse(s)) {
            return peer;
        }
        throw new IllegalArgumentException("Invalid peer str:" + s);
    }

    /**
     * Create a Endpoint instance from  a string in the form of "host:port",
     * returns null when string is blank.
     * 通过字符串直接生成 endpoint 对象
     */
    public static Endpoint getEndPoint(final String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        final String[] tmps = StringUtils.split(s, ':');
        if (tmps.length != 2) {
            throw new IllegalArgumentException("Invalid endpoint string: " + s);
        }
        return new Endpoint(tmps[0], Integer.parseInt(tmps[1]));
    }
}
