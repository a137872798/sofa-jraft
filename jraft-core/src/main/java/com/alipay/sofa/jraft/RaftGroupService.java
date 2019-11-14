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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.ProtobufMsgFactory;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;

/**
 * A framework to implement a raft group service.
 * raft 组级别的服务  该对象是初始化 node 组的起点 通过在opts 中设置 本节点地址 会完成 node 的初始化
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 7:53:03 PM
 */
public class RaftGroupService {

    private static final Logger LOG     = LoggerFactory.getLogger(RaftGroupService.class);

    static {
        // 协议工厂
        ProtobufMsgFactory.load();
    }

    /**
     * group 服务是否启动
     */
    private volatile boolean    started = false;

    /**
     * This node serverId
     * 该节点对应的 参与者id
     */
    private PeerId              serverId;

    /**
     * Node options
     */
    private NodeOptions         nodeOptions;

    /**
     * The raft RPC server
     * 通信服务对象
     */
    private RpcServer           rpcServer;

    /**
     * If we want to share the rpcServer instance, then we can't stop it when shutdown.
     */
    private final boolean       sharedRpcServer;

    /**
     * The raft group id
     */
    private String              groupId;
    /**
     * The raft node.
     * 当调用 start 后会设置该属性
     */
    private Node                node;

    public RaftGroupService(final String groupId, final PeerId serverId, final NodeOptions nodeOptions) {
        this(groupId, serverId, nodeOptions, RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint(),
            JRaftUtils.createExecutor("RAFT-RPC-executor-", nodeOptions.getRaftRpcThreadPoolSize()),
            JRaftUtils.createExecutor("CLI-RPC-executor-", nodeOptions.getCliRpcThreadPoolSize())));
    }

    /**
     * 起点
     * @param groupId  本节点对应的组id
     * @param serverId  本node对应的服务地址
     * @param nodeOptions
     * @param rpcServer
     */
    public RaftGroupService(final String groupId, final PeerId serverId, final NodeOptions nodeOptions,
                            final RpcServer rpcServer) {
        this(groupId, serverId, nodeOptions, rpcServer, false);
    }

    /**
     *
     * @param groupId
     * @param serverId
     * @param nodeOptions
     * @param rpcServer
     * @param sharedRpcServer  该标识如果为 true 那么调用shutdown 时 无法停止该 server
     */
    public RaftGroupService(final String groupId, final PeerId serverId, final NodeOptions nodeOptions,
                            final RpcServer rpcServer, final boolean sharedRpcServer) {
        super();
        this.groupId = groupId;
        this.serverId = serverId;
        this.nodeOptions = nodeOptions;
        this.rpcServer = rpcServer;
        this.sharedRpcServer = sharedRpcServer;
    }

    public synchronized Node getRaftNode() {
        return this.node;
    }

    /**
     * Starts the raft group service, returns the raft node.
     * 启动 raft 服务 并返回一个节点对象
     */
    public synchronized Node start() {
        return this.start(true);
    }

    /**
     * Starts the raft group service, returns the raft node.
     * 启动一个raft 组服务 并返回一个节点对象
     * 这里会顺带完成 node 的初始化
     * @param startRpcServer whether to start RPC server.  同时启动 rpc 服务器
     */
    public synchronized Node start(final boolean startRpcServer) {
        // 如果已经启动成功直接返回 node 对象
        if (this.started) {
            return this.node;
        }
        // 必要参数必须设定
        if (this.serverId == null || this.serverId.getEndpoint() == null
            || this.serverId.getEndpoint().equals(new Endpoint(Utils.IP_ANY, 0))) {
            throw new IllegalArgumentException("Blank serverId:" + this.serverId);
        }
        if (StringUtils.isBlank(this.groupId)) {
            throw new IllegalArgumentException("Blank group id" + this.groupId);
        }
        //Adds RPC server to Server.
        //将本服务对应的地址注册到NodeManager中  如果 NodeManager 是 针对JVM 级别的 那么 在本机启动的所有节点都会注册到这里
        NodeManager.getInstance().addAddress(this.serverId.getEndpoint());

        // 创建 node对象并执行init
        this.node = RaftServiceFactory.createAndInitRaftNode(this.groupId, this.serverId, this.nodeOptions);
        if (startRpcServer) {
            // 启动服务对象 便于接受其他node 的请求  这里就是 alipay.remoting 的事了 先不看 也就是netty那套吧
            this.rpcServer.start();
        } else {
            LOG.warn("RPC server is not started in RaftGroupService.");
        }
        this.started = true;
        LOG.info("Start the RaftGroupService successfully.");
        return this.node;
    }

    /**
     * Block thread to wait the server shutdown.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public synchronized void join() throws InterruptedException {
        if (this.node != null) {
            this.node.join();
        }
    }

    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.rpcServer != null) {
            try {
                if (!this.sharedRpcServer) {
                    this.rpcServer.stop();
                }
            } catch (final Exception e) {
                // ignore
            }
            this.rpcServer = null;
        }
        this.node.shutdown(status -> {
            synchronized (this) {
                this.node = null;
            }
        });
        NodeManager.getInstance().removeAddress(this.serverId.getEndpoint());
        this.started = false;
        LOG.info("Stop the RaftGroupService successfully.");
    }

    /**
     * Returns true when service is started.
     */
    public boolean isStarted() {
        return this.started;
    }

    /**
     * Returns the raft group id.
     */
    public String getGroupId() {
        return this.groupId;
    }

    /**
     * Set the raft group id
     */
    public void setGroupId(final String groupId) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        this.groupId = groupId;
    }

    /**
     * Returns the node serverId
     */
    public PeerId getServerId() {
        return this.serverId;
    }

    /**
     * Set the node serverId
     */
    public void setServerId(final PeerId serverId) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        this.serverId = serverId;
    }

    /**
     * Returns the node options.
     */
    public RpcOptions getNodeOptions() {
        return this.nodeOptions;
    }

    /**
     * Set node options.
     */
    public void setNodeOptions(final NodeOptions nodeOptions) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        if (nodeOptions == null) {
            throw new IllegalArgumentException("Invalid node options.");
        }
        nodeOptions.validate();
        this.nodeOptions = nodeOptions;
    }

    /**
     * Returns the rpc server instance.
     */
    public RpcServer getRpcServer() {
        return this.rpcServer;
    }

    /**
     * Set rpc server.
     */
    public void setRpcServer(final RpcServer rpcServer) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        if (this.serverId == null) {
            throw new IllegalStateException("Please set serverId at first");
        }
        if (rpcServer.port() != this.serverId.getPort()) {
            throw new IllegalArgumentException("RPC server port mismatch");
        }
        this.rpcServer = rpcServer;
    }
}
