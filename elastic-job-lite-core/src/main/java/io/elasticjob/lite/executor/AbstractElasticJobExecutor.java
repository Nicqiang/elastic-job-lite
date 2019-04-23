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

package io.elasticjob.lite.executor;

import io.elasticjob.lite.api.ShardingContext;
import io.elasticjob.lite.config.JobRootConfiguration;
import io.elasticjob.lite.event.type.JobExecutionEvent;
import io.elasticjob.lite.event.type.JobStatusTraceEvent.State;
import io.elasticjob.lite.exception.ExceptionUtil;
import io.elasticjob.lite.exception.JobExecutionEnvironmentException;
import io.elasticjob.lite.exception.JobSystemException;
import io.elasticjob.lite.executor.handler.ExecutorServiceHandler;
import io.elasticjob.lite.executor.handler.ExecutorServiceHandlerRegistry;
import io.elasticjob.lite.executor.handler.JobExceptionHandler;
import io.elasticjob.lite.executor.handler.JobProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * 弹性化分布式作业执行器.
 * 作业执行器抽象封装
 *
 * @author zhangliang
 */
@Slf4j
public abstract class AbstractElasticJobExecutor {

    /**
     * 作业门面封装
     */
    @Getter(AccessLevel.PROTECTED)
    private final JobFacade jobFacade;


    /**
     * 作业配置
     */
    @Getter(AccessLevel.PROTECTED)
    private final JobRootConfiguration jobRootConfig;

    /**
     * 作业名称
     */
    private final String jobName;

    /**
     * 作业执行线程池
     */
    private final ExecutorService executorService;

    /**
     * 作业异常处理器
     */
    private final JobExceptionHandler jobExceptionHandler;

    /**
     * 作业封片错误信息集合
     */
    private final Map<Integer, String> itemErrorMessages;
    
    protected AbstractElasticJobExecutor(final JobFacade jobFacade) {
        this.jobFacade = jobFacade;

        //加载作业配置
        this.jobRootConfig = jobFacade.loadJobRootConfiguration(true);

        this.jobName = jobRootConfig.getTypeConfig().getCoreConfig().getJobName();

        //获取执行线程并注册到，线程池服务处理器注册表中
        this.executorService = ExecutorServiceHandlerRegistry
                .getExecutorServiceHandler(jobName, (ExecutorServiceHandler) getHandler(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER));

        //获取作业 异常处理器，从JobProperties中获取
        jobExceptionHandler = (JobExceptionHandler) getHandler(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER);

        //设置作业分片错误集合
        itemErrorMessages = new ConcurrentHashMap<>(jobRootConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(), 1);
    }


    /**
     * 每个处理器都会对应一个JobPropertiesEnum，使用获取枚举获取处理器
     * 优先从JobProperties中获取自定义的处理器实现类，如果不符合条件则使用默认的的处理器实现
     * 每个作业可以配置不同的处理器
     * @param jobPropertiesEnum
     * @return
     */
    private Object getHandler(final JobProperties.JobPropertiesEnum jobPropertiesEnum) {
        String handlerClassName = jobRootConfig.getTypeConfig().getCoreConfig().getJobProperties().get(jobPropertiesEnum);
        try {
            Class<?> handlerClass = Class.forName(handlerClassName);
            if (jobPropertiesEnum.getClassType().isAssignableFrom(handlerClass)) {
                //必须实现
                return handlerClass.newInstance();
            }
            return getDefaultHandler(jobPropertiesEnum, handlerClassName);
        } catch (final ReflectiveOperationException ex) {
            return getDefaultHandler(jobPropertiesEnum, handlerClassName);
        }
    }
    
    private Object getDefaultHandler(final JobProperties.JobPropertiesEnum jobPropertiesEnum, final String handlerClassName) {
        log.warn("Cannot instantiation class '{}', use default '{}' class.", handlerClassName, jobPropertiesEnum.getKey());
        try {
            return Class.forName(jobPropertiesEnum.getDefaultValue()).newInstance();
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new JobSystemException(e);
        }
    }
    
    /**
     * 执行作业.
     * 作业中从方法
     */
    public final void execute() {
        try {
            //检查环境，当前仅检验作业执行环境，如果不合法且采用DefaultJobExceptionHandler作业异常处理，只打印日志，不会终止执行
            //如果当前作业对时间精度要求较高，期望终止，可以自定义异常处理
            jobFacade.checkJobExecutionEnvironment();
        } catch (final JobExecutionEnvironmentException cause) {
            jobExceptionHandler.handleException(jobName, cause);
        }

        //获取当前作业服务器的分片上下文
        ShardingContexts shardingContexts = jobFacade.getShardingContexts();
        if (shardingContexts.isAllowSendJobEvent()) {
            //发布作业状态追踪事件State.TASK_STAGING
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_STAGING, String.format("Job '%s' execute begin.", jobName));
        }

        //跳过存在运行中的被错过的作业
        if (jobFacade.misfireIfRunning(shardingContexts.getShardingItemParameters().keySet())) {
            if (shardingContexts.isAllowSendJobEvent()) {
                ////发布作业状态追踪事件State.TASK_FINISHED
                jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format(
                        "Previous job '%s' - shardingItems '%s' is still running, misfired job will start after previous job completed.", jobName, 
                        shardingContexts.getShardingItemParameters().keySet()));
            }
            return;
        }
        try {

            //作业执行前的方法
            jobFacade.beforeJobExecuted(shardingContexts);
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobExceptionHandler.handleException(jobName, cause);
        }

        //执行作业
        execute(shardingContexts, JobExecutionEvent.ExecutionSource.NORMAL_TRIGGER);

        //执行 被跳过的触发作业
        //为什么此处使用 while(…)？防御性编程，
        // #isExecuteMisfired(...) 判断使用内存缓存的数据，
        // 而该数据的更新依赖 Zookeeper 通知进行异步更新，可能因为各种情况，
        // 例如网络，数据可能未及时更新导致数据不一致。使用 while(…) 进行防御编程，保证内存缓存的数据已经更新。
        while (jobFacade.isExecuteMisfired(shardingContexts.getShardingItemParameters().keySet())) {
            //清除分配的作业分片项被错过执行的标识，并执行作业分片项
            jobFacade.clearMisfire(shardingContexts.getShardingItemParameters().keySet());
            execute(shardingContexts, JobExecutionEvent.ExecutionSource.MISFIRE);
        }

        //执行 作业失效转移
        jobFacade.failoverIfNecessary();


        //作业执行后的方法
        try {
            jobFacade.afterJobExecuted(shardingContexts);
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobExceptionHandler.handleException(jobName, cause);
        }
    }



    private void execute(final ShardingContexts shardingContexts, final JobExecutionEvent.ExecutionSource executionSource) {
        if (shardingContexts.getShardingItemParameters().isEmpty()) {
            if (shardingContexts.isAllowSendJobEvent()) {
                //post 状态
                jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format("Sharding item for job '%s' is empty.", jobName));
            }
            return;
        }

        //注册作业启动信息
        jobFacade.registerJobBegin(shardingContexts);

        String taskId = shardingContexts.getTaskId();
        if (shardingContexts.isAllowSendJobEvent()) {
            //post 状态
            jobFacade.postJobStatusTraceEvent(taskId, State.TASK_RUNNING, "");
        }
        try {
            process(shardingContexts, executionSource);
        } finally {
            // TODO 考虑增加作业失败的状态，并且考虑如何处理作业失败的整体回路
            jobFacade.registerJobCompleted(shardingContexts);

            //根据是否异有异常，发布作业状态事件追踪
            if (itemErrorMessages.isEmpty()) {
                if (shardingContexts.isAllowSendJobEvent()) {
                    jobFacade.postJobStatusTraceEvent(taskId, State.TASK_FINISHED, "");
                }
            } else {
                if (shardingContexts.isAllowSendJobEvent()) {
                    jobFacade.postJobStatusTraceEvent(taskId, State.TASK_ERROR, itemErrorMessages.toString());
                }
            }
        }
    }
    
    private void process(final ShardingContexts shardingContexts, final JobExecutionEvent.ExecutionSource executionSource) {
        Collection<Integer> items = shardingContexts.getShardingItemParameters().keySet();
        if (1 == items.size()) {
            int item = shardingContexts.getShardingItemParameters().keySet().iterator().next();
            JobExecutionEvent jobExecutionEvent =  new JobExecutionEvent(shardingContexts.getTaskId(), jobName, executionSource, item);
            process(shardingContexts, item, jobExecutionEvent);
            return;
        }
        final CountDownLatch latch = new CountDownLatch(items.size());
        for (final int each : items) {
            final JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(shardingContexts.getTaskId(), jobName, executionSource, each);
            if (executorService.isShutdown()) {
                return;
            }
            executorService.submit(new Runnable() {
                
                @Override
                public void run() {
                    try {
                        process(shardingContexts, each, jobExecutionEvent);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    
    private void process(final ShardingContexts shardingContexts, final int item, final JobExecutionEvent startEvent) {
        if (shardingContexts.isAllowSendJobEvent()) {
            jobFacade.postJobExecutionEvent(startEvent);
        }
        log.trace("Job '{}' executing, item is: '{}'.", jobName, item);
        JobExecutionEvent completeEvent;
        try {
            process(new ShardingContext(shardingContexts, item));
            completeEvent = startEvent.executionSuccess();
            log.trace("Job '{}' executed, item is: '{}'.", jobName, item);
            if (shardingContexts.isAllowSendJobEvent()) {
                jobFacade.postJobExecutionEvent(completeEvent);
            }
            // CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            // CHECKSTYLE:ON
            completeEvent = startEvent.executionFailure(cause);
            jobFacade.postJobExecutionEvent(completeEvent);
            itemErrorMessages.put(item, ExceptionUtil.transform(cause));
            jobExceptionHandler.handleException(jobName, cause);
        }
    }
    
    protected abstract void process(ShardingContext shardingContext);
}
