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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.example.counter.rpc.GetValueRequestProcessor;
import com.alipay.sofa.jraft.example.counter.rpc.IncrementAndGetRequestProcessor;
import com.alipay.sofa.jraft.example.counter.rpc.ValueResponse;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;

/**
 * Counter server that keeps a counter value in a raft group.
 * 该对象对应一个 raftGroup
 * 该对象应该是要启动多个的 每个对应到 一个node 然后他们的group 要一致才有意义
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Apr-09 4:51:02 PM
 */
public class CounterServer {

    /**
     * 内部存在一个 group 服务 要使用某个node 前必须确保 该对象被初始化
     */
    private RaftGroupService raftGroupService;
    private Node node;
    private CounterStateMachine fsm;

    /**
     * 在入参中已经定义了本节点的 地址 已经对应的group 中所有的节点地址信息
     *
     * @param dataPath
     * @param groupId
     * @param serverId
     * @param nodeOptions
     * @throws IOException
     */
    public CounterServer(final String dataPath, final String groupId, final PeerId serverId,
                         final NodeOptions nodeOptions) throws IOException {
        // 初始化路径
        // 对应logManager以及 快照元数据 快照文件 的 存储地址
        FileUtils.forceMkdir(new File(dataPath));

        // 这里让 raft RPC 和业务 RPC 使用同一个 RPC server, 通常也可以分开
        // 该对象对应了 CS 架构中 服务端的概念 node中会包含一个  server 用于接收follower 的请求
        final RpcServer rpcServer = new RpcServer(serverId.getPort());
        // 为该对象增加 默认的请求处理器
        RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);
        // 注册业务处理器   获取 针对获取counter 的请求
        rpcServer.registerUserProcessor(new GetValueRequestProcessor(this));
        // 增加counter 的请求
        rpcServer.registerUserProcessor(new IncrementAndGetRequestProcessor(this));
        // 初始化状态机 状态机由用户自己实现 主要是实现提交任务 保存快照(的逻辑) 下载快照(后的逻辑)
        this.fsm = new CounterStateMachine();
        // 设置状态机到启动参数
        nodeOptions.setFsm(this.fsm);
        // 设置存储路径
        // 日志, 必须
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // 元信息, 必须
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, 可选, 一般都推荐
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // 初始化 raft group 服务框架
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // 启动
        this.node = this.raftGroupService.start();
    }

    public CounterStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }

    /**
     * Redirect request to new leader
     * 这里返回了 指定的leader 避免下次返回相同的leader
     */
    public ValueResponse redirect() {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        if (this.node != null) {
            final PeerId leader = this.node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }
        return response;
    }

    /**
     * GoGoGo
     *
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
//        if (args.length != 4) {
//            System.out
//                    .println("Useage : java com.alipay.sofa.jraft.example.counter.CounterServer {dataPath} {groupId} {serverId} {initConf}");
//            System.out
//                    .println("Example: java com.alipay.sofa.jraft.example.counter.CounterServer /tmp/server1 counter 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
//            System.exit(1);
//        }
        final String dataPath = "/tmp/server1";
        final String groupId = "counter";
        final String serverIdStr = "127.0.0.1:8080";
        final String initConfStr = "127.0.0.1:8080";

        // 首先通过 命令行参数来设置 组 id 和 服务id  还有初始配置
//        final String dataPath = args[0];
//        final String groupId = args[1];
//        final String serverIdStr = args[2];
//        final String initConfStr = args[3];

        final NodeOptions nodeOptions = new NodeOptions();
        // 为了测试,调整 snapshot 间隔等参数
        // 设置选举超时时间为 1 秒
        nodeOptions.setElectionTimeoutMs(1000);
        // 关闭 CLI 服务。
        nodeOptions.setDisableCli(false);
        // 每隔30秒做一次 snapshot
        nodeOptions.setSnapshotIntervalSecs(30);
        // 解析参数
        final PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        // 原来是这样设置的  一开始 在 serverIdStr 中已经 指定了本节点的 ip port 然后在 conf中 定义了 本group 初始状态的所有node 地址信息
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // 设置初始集群配置
        nodeOptions.setInitialConf(initConf);

        // 启动
        final CounterServer counterServer = new CounterServer(dataPath, groupId, serverId, nodeOptions);

        System.out.println("Started counter server at port:"
                + counterServer.getNode().getNodeId().getPeerId().getPort());
    }
}
