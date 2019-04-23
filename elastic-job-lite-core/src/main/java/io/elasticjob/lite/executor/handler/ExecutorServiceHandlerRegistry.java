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

package io.elasticjob.lite.executor.handler;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * 线程池服务处理器注册表.
 * 作业每次执行时，可能需要分配多个分片项，需要使用线程池实现并行执行
 * 考虑到不同作业之间的隔离性，通过一个作业的一个线程池实现。
 * 线程池服务处理器注册表中获取线程池
 * 
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ExecutorServiceHandlerRegistry {
    
    private static final Map<String, ExecutorService> REGISTRY = new HashMap<>();
    
    /**
     * 获取线程池服务.
     * 
     * @param jobName 作业名称
     * @param executorServiceHandler 线程池服务处理器
     * @return 线程池服务
     */
    public static synchronized ExecutorService getExecutorServiceHandler(final String jobName, final ExecutorServiceHandler executorServiceHandler) {
        //
        if (!REGISTRY.containsKey(jobName)) {
            //调用createExecutorService创建线程池服务对象
            REGISTRY.put(jobName, executorServiceHandler.createExecutorService(jobName));
        }
        return REGISTRY.get(jobName);
    }
    
    /**
     * 从注册表中删除该作业线程池服务.
     *
     * @param jobName 作业名称
     */
    public static synchronized void remove(final String jobName) {
        REGISTRY.remove(jobName);
    }
}
