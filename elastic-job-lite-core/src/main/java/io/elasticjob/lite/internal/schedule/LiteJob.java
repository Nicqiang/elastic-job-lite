package io.elasticjob.lite.internal.schedule;

import io.elasticjob.lite.api.ElasticJob;
import io.elasticjob.lite.executor.JobExecutorFactory;
import io.elasticjob.lite.executor.JobFacade;
import lombok.Setter;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Lite调度作业.
 *
 * Quartz到达调度时间时，会创建该对象进行执行#execute()
 *
 * @author zhangliang
 */
public final class LiteJob implements Job {
    
    @Setter
    private ElasticJob elasticJob;
    
    @Setter
    private JobFacade jobFacade;
    
    @Override
    public void execute(final JobExecutionContext context) throws JobExecutionException {
        //通过JobExecutorFactory获取作业执行器AbstractElasticJobExecutor，并进行执行
        JobExecutorFactory.getJobExecutor(elasticJob, jobFacade).execute();
    }
}
