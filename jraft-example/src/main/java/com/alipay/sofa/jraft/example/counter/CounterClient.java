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
package com.alipay.sofa.jraft.example.counter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.example.counter.rpc.IncrementAndGetRequest;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;

/**
 * jraft 只是一个 底层框架  其他框架在使用它时 要自己实现状态机 已经 对应的 server client
 * 这里的server 每个对应到一个 node 然后对应就对应到一个 raft组 同时创建一个 client 用于访问 leader 对象 通过 routeTable 查找组中的 leader 并提交任务
 * 然后通过raft组实现 分布式一致性
 */
public class CounterClient {

    /**
     * OgOgOg
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
//        if (args.length != 2) {
//            System.out.println("Useage : java com.alipay.sofa.jraft.example.counter.CounterClient {groupId} {conf}");
//            System.out
//                .println("Example: java com.alipay.sofa.jraft.example.counter.CounterClient counter 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
//            System.exit(1);
//        }

        final String groupId = "counter";
        final String confStr = "127.0.0.1:8080";

        // 需要指定选择的 raft组id
//        final String groupId = args[0];
        // 在client中
//        final String confStr = args[1];

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        // 将当前组配置信息 注册到路由表中  可以从该对象中获取到 leader 地址
        RouteTable.getInstance().updateConfiguration(groupId, conf);

        // 生成命令行客户端  可以通过该对象直接 更改集群中的leader 或者 移除/增加 节点
        final BoltCliClientService cliClientService = new BoltCliClientService();
        // 初始化 业务线程池  netty 的线程模型只是用来接受请求的 实际执行任务需要派发给业务线程池
        cliClientService.init(new CliOptions());

        // 开始搜索leader 内部通过cliClient 发送请求访问node 获取leaderId
        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        final int n = 1000;
        final CountDownLatch latch = new CountDownLatch(n);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            incrementAndGet(cliClientService, leader, i, latch);
        }
        // 如果失败的话 可以更换node (在响应结果中设置本次失败的leader 然后 再次调用refreshLeader 同时设置leader 这样就不会获取到相同的leader)

        // 通过阻塞模拟业务处理
        latch.await();
        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
        while (true){}
//        System.exit(0);
    }

    /**
     * 这里主要看 如何向leader 提交任务  针对多个请求 是否一定会在某个任务成功提交后才提交下一个
     * @param cliClientService
     * @param leader
     * @param delta
     * @param latch
     * @throws RemotingException
     * @throws InterruptedException
     */
    private static void incrementAndGet(final BoltCliClientService cliClientService, final PeerId leader,
                                        final long delta, CountDownLatch latch) throws RemotingException,
                                                                               InterruptedException {
        // 应该是在 server 已经设置了状态机 然后设置一个 processor 这里用户发送自定义请求然后 对端指定处理逻辑 在那里使用状态机去处理任务
        final IncrementAndGetRequest request = new IncrementAndGetRequest();
        request.setDelta(delta);
        cliClientService.getRpcClient().invokeWithCallback(leader.getEndpoint().toString(), request,
            new InvokeCallback() {

                @Override
                public void onResponse(Object result) {
                    latch.countDown();
                    System.out.println("incrementAndGet result:" + result);
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    latch.countDown();

                }

                @Override
                public Executor getExecutor() {
                    return null;
                }
            }, 5000);
    }

}
