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

package io.elasticjob.lite.api;

import com.google.common.base.Optional;
import io.elasticjob.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import io.elasticjob.lite.api.listener.ElasticJobListener;
import io.elasticjob.lite.api.script.ScriptJob;
import io.elasticjob.lite.api.strategy.JobInstance;
import io.elasticjob.lite.config.LiteJobConfiguration;
import io.elasticjob.lite.event.JobEventBus;
import io.elasticjob.lite.event.JobEventConfiguration;
import io.elasticjob.lite.exception.JobConfigurationException;
import io.elasticjob.lite.exception.JobSystemException;
import io.elasticjob.lite.executor.JobFacade;
import io.elasticjob.lite.internal.guarantee.GuaranteeService;
import io.elasticjob.lite.internal.schedule.JobRegistry;
import io.elasticjob.lite.internal.schedule.JobScheduleController;
import io.elasticjob.lite.internal.schedule.JobShutdownHookPlugin;
import io.elasticjob.lite.internal.schedule.LiteJob;
import io.elasticjob.lite.internal.schedule.LiteJobFacade;
import io.elasticjob.lite.internal.schedule.SchedulerFacade;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import lombok.Getter;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 作业调度器.
 * 
 * @author zhangliang
 * @author caohao
 */
public class JobScheduler {
    
    public static final String ELASTIC_JOB_DATA_MAP_KEY = "elasticJob";
    
    private static final String JOB_FACADE_DATA_MAP_KEY = "jobFacade";

    /**
     * 作业配置中心
     */
    private final LiteJobConfiguration liteJobConfig;

    /**
     * 作业注册中心
     */
    private final CoordinatorRegistryCenter regCenter;

    /**
     * 调度器门面对象
     */
    // TODO 为测试使用,测试用例不能反复new monitor service,以后需要把MonitorService重构为单例
    @Getter
    private final SchedulerFacade schedulerFacade;

    /**
     * 作业内部服务门面服务
     */
    private final JobFacade jobFacade;
    
    public JobScheduler(final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration liteJobConfig, final ElasticJobListener... elasticJobListeners) {
        this(regCenter, liteJobConfig, new JobEventBus(), elasticJobListeners);
    }
    
    public JobScheduler(final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration liteJobConfig, final JobEventConfiguration jobEventConfig, 
                        final ElasticJobListener... elasticJobListeners) {
        this(regCenter, liteJobConfig, new JobEventBus(jobEventConfig), elasticJobListeners);
    }


    /**
     * 构造
     * @param regCenter 注册中心
     * @param liteJobConfig 配置
     * @param jobEventBus event bus
     * @param elasticJobListeners 监听器
     */
    private JobScheduler(final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration liteJobConfig, final JobEventBus jobEventBus, final ElasticJobListener... elasticJobListeners) {
        //添加作业运行实例
        JobRegistry.getInstance().addJobInstance(liteJobConfig.getJobName(), new JobInstance());
        this.liteJobConfig = liteJobConfig;
        this.regCenter = regCenter;
        List<ElasticJobListener> elasticJobListenerList = Arrays.asList(elasticJobListeners);

        //设置作业监听
        setGuaranteeServiceForElasticJobListeners(regCenter, elasticJobListenerList);

        //设置调度器门面对象
        this.schedulerFacade = new SchedulerFacade(regCenter, liteJobConfig.getJobName(), elasticJobListenerList);

        //设置作业门面对象
        this.jobFacade = new LiteJobFacade(regCenter, liteJobConfig.getJobName(), Arrays.asList(elasticJobListeners), jobEventBus);
    }
    
    private void setGuaranteeServiceForElasticJobListeners(final CoordinatorRegistryCenter regCenter, final List<ElasticJobListener> elasticJobListeners) {
        GuaranteeService guaranteeService = new GuaranteeService(regCenter, liteJobConfig.getJobName());
        for (ElasticJobListener each : elasticJobListeners) {
            if (each instanceof AbstractDistributeOnceElasticJobListener) {
                ((AbstractDistributeOnceElasticJobListener) each).setGuaranteeService(guaranteeService);
            }
        }
    }
    
    /**
     * 初始化作业.
     * 作业调度器创建后，调用init，开始作业调度
     */
    public void init() {
        //更新作业配置
        LiteJobConfiguration liteJobConfigFromRegCenter = schedulerFacade.updateJobConfiguration(liteJobConfig);

        //设置作业分片总数
        JobRegistry.getInstance().setCurrentShardingTotalCount(liteJobConfigFromRegCenter.getJobName(), liteJobConfigFromRegCenter.getTypeConfig().getCoreConfig().getShardingTotalCount());

        //初始化作业调度控制器
        JobScheduleController jobScheduleController = new JobScheduleController(
                createScheduler(), createJobDetail(liteJobConfigFromRegCenter.getTypeConfig().getJobClass()), liteJobConfigFromRegCenter.getJobName());

        //添加作业调度控制器
        JobRegistry.getInstance().registerJob(liteJobConfigFromRegCenter.getJobName(), jobScheduleController, regCenter);

        //注册作业的启动信息
        schedulerFacade.registerStartUpInfo(!liteJobConfigFromRegCenter.isDisabled());

        //调度作业
        jobScheduleController.scheduleJob(liteJobConfigFromRegCenter.getTypeConfig().getCoreConfig().getCron());
    }


    /**
     * 创建quartz的作业{@link JobDetail}
     * @param jobClass
     * @return
     */
    private JobDetail createJobDetail(final String jobClass) {
        JobDetail result = JobBuilder.newJob(LiteJob.class).withIdentity(liteJobConfig.getJobName()).build();

        //作业门面信息放入JobDataMap中
        result.getJobDataMap().put(JOB_FACADE_DATA_MAP_KEY, this.jobFacade);

        //创建elastic-job实例，如果已经创建好springJobScheduler，则无需进行创建
        Optional<ElasticJob> elasticJobInstance = createElasticJobInstance();
        if (elasticJobInstance.isPresent()) {
            result.getJobDataMap().put(ELASTIC_JOB_DATA_MAP_KEY, elasticJobInstance.get());
        } else if (!jobClass.equals(ScriptJob.class.getCanonicalName())) {
            try {
                result.getJobDataMap().put(ELASTIC_JOB_DATA_MAP_KEY, Class.forName(jobClass).newInstance());
            } catch (final ReflectiveOperationException ex) {
                throw new JobConfigurationException("Elastic-Job: Job class '%s' can not initialize.", jobClass);
            }
        }
        return result;
    }
    
    protected Optional<ElasticJob> createElasticJobInstance() {
        return Optional.absent();
    }


    /**
     * 创建quartz的调度器
     * @return
     */
    private Scheduler createScheduler() {
        Scheduler result;
        try {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            factory.initialize(getBaseQuartzProperties());
            result = factory.getScheduler();
            result.getListenerManager().addTriggerListener(schedulerFacade.newJobTriggerListener());
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
        return result;
    }

    /**
     * 调度器配置信息
     * @return
     */
    private Properties getBaseQuartzProperties() {
        Properties result = new Properties();
        //线程池类
        result.put("org.quartz.threadPool.class", org.quartz.simpl.SimpleThreadPool.class.getName());
        //设置线程数为1，一个作业的调度，需要配置独有的一个作业调度器（JobScheduler),两者之间的关系是1:1关系
        result.put("org.quartz.threadPool.threadCount", "1");
        result.put("org.quartz.scheduler.instanceName", liteJobConfig.getJobName());
        result.put("org.quartz.jobStore.misfireThreshold", "1");
        //作业关闭的钩子
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());

        //关闭时，清理所有的资源
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }
}
