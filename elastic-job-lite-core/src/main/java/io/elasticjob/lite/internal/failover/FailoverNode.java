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

package io.elasticjob.lite.internal.failover;

import io.elasticjob.lite.internal.election.LeaderNode;
import io.elasticjob.lite.internal.sharding.ShardingNode;
import io.elasticjob.lite.internal.storage.JobNodePath;

/**
 * 失效转移节点路径.
 * 这里大家可能会和我一样比较疑惑，为什么 /leader/failover 放在 /leader 目录下，而不独立成为一个根目录？经过确认，作业失效转移 设计到分布式锁，统一存储在 /leader 目录下
 *
 * [zk: localhost:2181(CONNECTED) 2] ls /elastic-job-example-lite-java/javaSimpleJob/leader/failover
 * [latch, items]
 * [zk: localhost:2181(CONNECTED) 4] ls /elastic-job-example-lite-java/javaSimpleJob/leader/failover/items
 * [0]
 *
 * /leader/failover/latch 作业失效转移分布式锁，和 /leader/failover/latch 是一致的。
 * /leader/items/${ITEM_ID} 是永久节点，当某台作业节点 CRASH 时，其分配的作业分片项标记需要进行失效转移，
 * 存储其分配的作业分片项的 /leader/items/${ITEM_ID} 为空串( "" )；
 * 当失效转移标记，移除 /leader/items/${ITEM_ID}，存储 /sharding/${ITEM_ID}/failover 为空串( "" )，临时节点，需要进行失效转移执行。
 * 《Elastic-Job-Lite 源码分析 —— 作业失效转移》详细解析。
 *
 * 
 * @author zhangliang
 */
public final class FailoverNode {
    
    static final String FAILOVER = "failover";
    
    static final String LEADER_ROOT = LeaderNode.ROOT + "/" + FAILOVER;
    
    static final String ITEMS_ROOT = LEADER_ROOT + "/items";
    
    static final String ITEMS = ITEMS_ROOT + "/%s";
    
    static final String LATCH = LEADER_ROOT + "/latch";
    
    private static final String EXECUTION_FAILOVER = ShardingNode.ROOT + "/%s/" + FAILOVER;
    
    private final JobNodePath jobNodePath;
    
    public FailoverNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }
    
    static String getItemsNode(final int item) {
        return String.format(ITEMS, item);
    }
    
    static String getExecutionFailoverNode(final int item) {
        return String.format(EXECUTION_FAILOVER, item);
    }
    
    /**
     * 根据失效转移执行路径获取分片项.
     * 
     * @param path 失效转移执行路径
     * @return 分片项, 不是失效转移执行路径获则返回null
     */
    public Integer getItemByExecutionFailoverPath(final String path) {
        if (!isFailoverPath(path)) {
            return null;
        }
        return Integer.parseInt(path.substring(jobNodePath.getFullPath(ShardingNode.ROOT).length() + 1, path.lastIndexOf(FailoverNode.FAILOVER) - 1));
    }
    
    private boolean isFailoverPath(final String path) {
        return path.startsWith(jobNodePath.getFullPath(ShardingNode.ROOT)) && path.endsWith(FailoverNode.FAILOVER);
    }
}
