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
package com.alipay.sofa.jraft.rhea.client.pd;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.RegionEngine;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Processing the instructions from the placement driver server.
 * 指令处理器
 * @author jiachun.fjc
 */
public class InstructionProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(InstructionProcessor.class);

    /**
     * 存储引擎
     */
    private final StoreEngine   storeEngine;

    public InstructionProcessor(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
    }

    /**
     * 处理一组命令
     * @param instructions
     */
    public void process(final List<Instruction> instructions) {
        LOG.info("Received instructions: {}.", instructions);
        for (final Instruction instruction : instructions) {

            // 无效命令跳过 (指null或者 region == null 的命令)
            if (!checkInstruction(instruction)) {
                continue;
            }
            // 处理针对拆分的命令
            processSplit(instruction);
            processTransferLeader(instruction);
        }
    }

    private boolean processSplit(final Instruction instruction) {
        try {
            // 获取命令中 区域拆分相关的对象
            final Instruction.RangeSplit rangeSplit = instruction.getRangeSplit();
            if (rangeSplit == null) {
                return false;
            }
            final Long newRegionId = rangeSplit.getNewRegionId();
            if (newRegionId == null) {
                LOG.error("RangeSplit#newRegionId must not be null, {}.", instruction);
                return false;
            }

            // 获取命令的 region 信息
            final Region region = instruction.getRegion();
            final long regionId = region.getId();
            // 获取该地区对应的存储引擎 该对象是执行 拷贝 的最小单位
            final RegionEngine engine = this.storeEngine.getRegionEngine(regionId);
            if (engine == null) {
                LOG.error("Could not found regionEngine, {}.", instruction);
                return false;
            }
            // 校验有效性
            if (!region.equals(engine.getRegion())) {
                LOG.warn("Instruction [{}] is out of date.", instruction);
                return false;
            }
            final CompletableFuture<Status> future = new CompletableFuture<>();
            // 委托给 storeEngine 来执行 拆分的逻辑
            this.storeEngine.applySplit(regionId, newRegionId, new BaseKVStoreClosure() {

                // 执行完成后将结果设置到 future中
                @Override
                public void run(Status status) {
                    future.complete(status);
                }
            });
            // 阻塞当前线程20秒 直到处理完成
            final Status status = future.get(20, TimeUnit.SECONDS);
            final boolean ret = status.isOk();
            if (ret) {
                LOG.info("Range-split succeeded, instruction: {}.", instruction);
            } else {
                LOG.warn("Range-split failed: {}, instruction: {}.", status, instruction);
            }
            return ret;
        } catch (final Throwable t) {
            LOG.error("Caught an exception on #processSplit: {}.", StackTraceUtil.stackTrace(t));
            return false;
        }
    }

    /**
     * 处理转换leader 的逻辑 同样是通过regionId 找到 regionEngine 之后委托给引擎对象 切换leader
     * @param instruction
     * @return
     */
    private boolean processTransferLeader(final Instruction instruction) {
        try {
            final Instruction.TransferLeader transferLeader = instruction.getTransferLeader();
            if (transferLeader == null) {
                return false;
            }
            final Endpoint toEndpoint = transferLeader.getMoveToEndpoint();
            if (toEndpoint == null) {
                LOG.error("TransferLeader#toEndpoint must not be null, {}.", instruction);
                return false;
            }
            final Region region = instruction.getRegion();
            final long regionId = region.getId();
            final RegionEngine engine = this.storeEngine.getRegionEngine(regionId);
            if (engine == null) {
                LOG.error("Could not found regionEngine, {}.", instruction);
                return false;
            }
            if (!region.equals(engine.getRegion())) {
                LOG.warn("Instruction [{}] is out of date.", instruction);
                return false;
            }
            return engine.transferLeadershipTo(toEndpoint);
        } catch (final Throwable t) {
            LOG.error("Caught an exception on #processTransferLeader: {}.", StackTraceUtil.stackTrace(t));
            return false;
        }
    }

    /**
     * 检测命令有效性
     * @param instruction
     * @return
     */
    private boolean checkInstruction(final Instruction instruction) {
        if (instruction == null) {
            LOG.warn("Null instructions element.");
            return false;
        }
        if (instruction.getRegion() == null) {
            LOG.warn("Null region with instruction: {}.", instruction);
            return false;
        }
        return true;
    }
}
