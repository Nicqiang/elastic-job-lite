/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.elasticjob.lite.internal.sharding;

import io.elasticjob.lite.api.strategy.JobInstance;
import io.elasticjob.lite.api.strategy.JobShardingStrategy;
import io.elasticjob.lite.api.strategy.JobShardingStrategyFactory;
import io.elasticjob.lite.config.LiteJobConfiguration;
import io.elasticjob.lite.internal.config.ConfigurationService;
import io.elasticjob.lite.internal.election.LeaderService;
import io.elasticjob.lite.internal.instance.InstanceNode;
import io.elasticjob.lite.internal.instance.InstanceService;
import io.elasticjob.lite.internal.schedule.JobRegistry;
import io.elasticjob.lite.internal.server.ServerService;
import io.elasticjob.lite.internal.storage.JobNodePath;
import io.elasticjob.lite.internal.storage.JobNodeStorage;
import io.elasticjob.lite.internal.storage.TransactionExecutionCallback;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import io.elasticjob.lite.util.concurrent.BlockUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 作业分片服务.
 *
 * 当作业满足分片条件时，不会立即进行作业分片分配，而是设置需要重新进行分片的标记，等到作业分片获取时，判断有该标记后执行作业分配
 * 
 * @author zhangliang
 */
@Slf4j
public final class ShardingService {

    /**
     * 作业名称
     */
    private final String jobName;

    /**
     * node storage
     */
    private final JobNodeStorage jobNodeStorage;

    /**
     * 选举
     */
    private final LeaderService leaderService;

    /**
     * 配置服务
     */
    private final ConfigurationService configService;

    /**
     * 作业运行实例服务
     */
    private final InstanceService instanceService;

    /**
     * 作业服务器服务
     */
    private final ServerService serverService;

    /**
     * 执行作业的服务
     */
    private final ExecutionService executionService;

    /**
     * 作业节点路径类
     */
    private final JobNodePath jobNodePath;
    
    public ShardingService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * 设置需要重新分片的标记.
     * /${JOB_NAME}/leader/sharding/necessary。该 Zookeeper 数据节点是永久节点，存储空串( "" )
     *
     * [zk: localhost:2181(CONNECTED) 2] ls /elastic-job-example-lite-java/javaSimpleJob/leader/sharding
     * [necessary]
     * [zk: localhost:2181(CONNECTED) 3] get /elastic-job-example-lite-java/javaSimpleJob/leader/sharding/necessary
     *
     * 四种分片的情况
     * 1、注册作业启动信息时
     * 2、作业分片总数( JobCoreConfiguration.shardingTotalCount )变化时
     * 3、服务器变化时
     *
     */
    public void setReshardingFlag() {
        jobNodeStorage.createJobNodeIfNeeded(ShardingNode.NECESSARY);
    }
    
    /**
     * 判断是否需要重分片.
     * 
     * @return 是否需要重分片
     */
    public boolean isNeedSharding() {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY);
    }
    
    /**
     * 如果需要分片且当前节点为主节点, 则作业分片.
     * 
     * <p>
     * 如果当前无可用节点则不分片.
     * </p>
     */
    public void shardingIfNecessary() {
        List<JobInstance> availableJobInstances = instanceService.getAvailableJobInstances();
        //判断是否需要分片
        if (!isNeedSharding() || availableJobInstances.isEmpty()) {
            return;
        }
        //// 【非主节点】等待 作业分片项分配完成
        if (!leaderService.isLeaderUntilBlock()) {
            blockUntilShardingCompleted();
            return;
        }

        //以下是主节点的逻辑

        //// 【主节点】作业分片项分配
        //   // 等待 作业未在运行中状态
        waitingOtherShardingItemCompleted();

        //从注册中心获取作业配置( 非缓存 )，避免主节点本地作业配置可能非最新的，主要目的是获得作业分片总数( shardingTotalCount )
        LiteJobConfiguration liteJobConfig = configService.load(false);
        int shardingTotalCount = liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount();
        log.debug("Job '{}' sharding begin.", jobName);

        //// 设置 作业正在重分片的标记
        //调用 jobNodeStorage.fillEphemeralJobNode(ShardingNode.PROCESSING, "") 设
        // 置作业正在重分片的标记 /${JOB_NAME}/leader/sharding/processing。
        // 该 Zookeeper 数据节点是临时节点，存储空串( "" )，仅用于标记作业正在重分片，无特别业务逻辑。
        jobNodeStorage.fillEphemeralJobNode(ShardingNode.PROCESSING, "");

        //// 重置 作业分片项信息
        resetShardingInfo(shardingTotalCount);

        //// 【事务中】设置 作业分片项信息
        JobShardingStrategy jobShardingStrategy = JobShardingStrategyFactory.getStrategy(liteJobConfig.getJobShardingStrategyClass());
        jobNodeStorage.executeInTransaction(new PersistShardingInfoTransactionExecutionCallback(jobShardingStrategy.sharding(availableJobInstances, jobName, shardingTotalCount)));
        log.debug("Job '{}' sharding complete.", jobName);
    }

    /**
     * 作业需要重分片的标记、作业正在重分片的标记 都不存在时，意味着作业分片项分配已经完成
     */
    private void blockUntilShardingCompleted() {
        while (!leaderService.isLeaderUntilBlock() // 当前作业节点不为【主节点】
                && (jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY)  // 存在作业需要重分片的标记
                || jobNodeStorage.isJobNodeExisted(ShardingNode.PROCESSING))) {
            log.debug("Job '{}' sleep short time until sharding completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }
    
    private void waitingOtherShardingItemCompleted() {
        while (executionService.hasRunningItems()) {
            log.debug("Job '{}' sleep short time until other job completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }
    
    private void resetShardingInfo(final int shardingTotalCount) {
        //// 重置 有效的作业分片项
        for (int i = 0; i < shardingTotalCount; i++) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getInstanceNode(i));
            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.ROOT + "/" + i);
        }
        //// 移除 多余的作业分片项
        int actualShardingTotalCount = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT).size();
        if (actualShardingTotalCount > shardingTotalCount) {
            for (int i = shardingTotalCount; i < actualShardingTotalCount; i++) {
                jobNodeStorage.removeJobNodeIfExisted(ShardingNode.ROOT + "/" + i);
            }
        }
    }
    
    /**
     * 获取作业运行实例的分片项集合.
     *
     * @param jobInstanceId 作业运行实例主键
     * @return 作业运行实例的分片项集合
     */
    public List<Integer> getShardingItems(final String jobInstanceId) {
        JobInstance jobInstance = new JobInstance(jobInstanceId);
        if (!serverService.isAvailableServer(jobInstance.getIp())) {
            return Collections.emptyList();
        }
        List<Integer> result = new LinkedList<>();
        int shardingTotalCount = configService.load(true).getTypeConfig().getCoreConfig().getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (jobInstance.getJobInstanceId().equals(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                result.add(i);
            }
        }
        return result;
    }
    
    /**
     * 获取运行在本作业实例的分片项集合.
     * ShardingService#getLocalShardingItems()方法，获得分配在本机的作业分片项，
     * 即 /${JOB_NAME}/sharding/${ITEM_ID}/instance 为本机的作业分片项
     * 
     * @return 运行在本作业实例的分片项集合
     */
    public List<Integer> getLocalShardingItems() {
        if (JobRegistry.getInstance().isShutdown(jobName) || !serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp())) {
            return Collections.emptyList();
        }
        return getShardingItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * 查询是包含有分片节点的不在线服务器.
     * 
     * @return 是包含有分片节点的不在线服务器
     */
    public boolean hasShardingInfoInOfflineServers() {
        List<String> onlineInstances = jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT);
        int shardingTotalCount = configService.load(true).getTypeConfig().getCoreConfig().getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (!onlineInstances.contains(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                return true;
            }
        }
        return false;
    }
    
    @RequiredArgsConstructor
    class PersistShardingInfoTransactionExecutionCallback implements TransactionExecutionCallback {

        /**
         * 作业分片项分配结果
         * key：作业节点
         * value：作业分片项
         */
        private final Map<JobInstance, List<Integer>> shardingResults;
        
        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            //设置 每个节点分配的作业分片项
            for (Map.Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) {
                for (int shardingItem : entry.getValue()) {
                    curatorTransactionFinal.create().forPath(jobNodePath.getFullPath(ShardingNode.getInstanceNode(shardingItem)), entry.getKey().getJobInstanceId().getBytes()).and();
                }
            }
            //删除NECESSARY,PROCESSING标记分片完成
            curatorTransactionFinal.delete().forPath(jobNodePath.getFullPath(ShardingNode.NECESSARY)).and();
            curatorTransactionFinal.delete().forPath(jobNodePath.getFullPath(ShardingNode.PROCESSING)).and();
        }
    }
}
