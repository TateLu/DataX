package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractScheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractScheduler.class);

    private ErrorRecordChecker errorLimit;

    private AbstractContainerCommunicator containerCommunicator;

    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public AbstractScheduler(AbstractContainerCommunicator containerCommunicator) {
        this.containerCommunicator = containerCommunicator;
    }

    /**
     * 负责调度数据同步任务
     * 该方法根据配置项初始化任务，监控并管理任务的执行状态，直到任务成功完成或失败
     *
     * @param configurations 包含所有任务配置信息的列表，不能为空
     * @throws DataXException 如果检测到任务执行过程中的异常错误，则抛出自定义异常
     */
    public void schedule(List<Configuration> configurations) {
        // 确保配置信息不为空
        Validate.notNull(configurations, "scheduler配置不能为空");

        // 获取任务报告间隔和任务休眠间隔配置
        int jobReportIntervalInMillSec = configurations.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);
        int jobSleepIntervalInMillSec = configurations.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);

        // 获取当前任务的ID
        this.jobId = configurations.get(0).getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);

        // 初始化错误记录检查器
        errorLimit = new ErrorRecordChecker(configurations.get(0));

        // 为任务组容器的通信注册配置
        this.containerCommunicator.registerCommunication(configurations);

        // 计算总任务数
        int totalTasks = calculateTaskCount(configurations);
        // 启动所有任务组
        startAllTaskGroup(configurations);

        // 初始化上次任务容器通信状态
        Communication lastJobContainerCommunication = new Communication();

        // 记录上次报告的时间戳
        long lastReportTimeStamp = System.currentTimeMillis();
        try {
            while (true) {
                // 收集当前任务的通信状态
                Communication nowJobContainerCommunication = this.containerCommunicator.collect();
                nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                LOG.debug(nowJobContainerCommunication.toString());

                // 检查是否达到汇报周期，达到则向数据服务平台汇报信息
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > jobReportIntervalInMillSec) {
                    Communication reportCommunication = CommunicationTool
                            .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);

                    this.containerCommunicator.report(reportCommunication);
                    lastReportTimeStamp = now;
                    lastJobContainerCommunication = nowJobContainerCommunication;
                }

                // 检查错误记录是否超出限制
                errorLimit.checkRecordLimit(nowJobContainerCommunication);

                // 如果所有任务成功完成，退出循环
                if (nowJobContainerCommunication.getState() == State.SUCCEEDED) {
                    LOG.info("Scheduler accomplished all tasks.");
                    break;
                }

                // 如果任务被终止或失败，处理对应状态
                if (isJobKilling(this.getJobId())) {
                    dealKillingStat(this.containerCommunicator, totalTasks);
                } else if (nowJobContainerCommunication.getState() == State.FAILED) {
                    dealFailedStat(this.containerCommunicator, nowJobContainerCommunication.getThrowable());
                }

                // 按配置的休眠间隔暂停线程
                Thread.sleep(jobSleepIntervalInMillSec);
            }
        } catch (InterruptedException e) {
            // 如果线程被中断，记录错误日志并抛出自定义异常
            LOG.error("捕获到InterruptedException异常!", e);
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    protected abstract void startAllTaskGroup(List<Configuration> configurations);

    protected abstract void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable);

    protected abstract void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks);

    private int calculateTaskCount(List<Configuration> configurations) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : configurations) {
            totalTasks += taskGroupConfiguration.getListConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

//    private boolean isJobKilling(Long jobId) {
//        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
//        return jobInfo.getData() == State.KILLING.value();
//    }

    protected  abstract  boolean isJobKilling(Long jobId);
}
