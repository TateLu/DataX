package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.TransformerUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * TaskGroupContainer类继承自AbstractContainer，代表一个任务组容器
 * 这个类主要用于管理和存储一组相关的任务，提供给这些任务在执行过程中所需的环境和资源
 */
public class TaskGroupContainer extends AbstractContainer {

    private static final Logger LOG = LoggerFactory
            .getLogger(TaskGroupContainer.class);

    /**
     * 当前taskGroup所属jobId
     */
    private long jobId;

    /**
     * 当前taskGroupId
     */
    private int taskGroupId;

    /**
     * 使用的channel类
     */
    private String channelClazz;

    /**
     * task收集器使用的类
     */
    private String taskCollectorClass;

    private TaskMonitor taskMonitor = TaskMonitor.getInstance();

    public TaskGroupContainer(Configuration configuration) {
        super(configuration);

        initCommunicator(configuration);

        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);

        this.channelClazz = this.configuration.getString(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        this.taskCollectorClass = this.configuration.getString(
                CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    private void initCommunicator(Configuration configuration) {
        super.setContainerCommunicator(new StandaloneTGContainerCommunicator(configuration));

    }

    public long getJobId() {
        return jobId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    @Override
    public void start() {
        try {
            /**
             * 状态check时间间隔，较短，可以把任务及时分发到对应channel中
             */
            int sleepIntervalInMillSec = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);
            /**
             * 状态汇报时间间隔，稍长，避免大量汇报
             */
            long reportIntervalInMillSec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL,
                    10000);
            /**
             * 2分钟汇报一次性能统计
             */

            // 获取channel数目
            // 从配置中获取通道数量
            int channelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);

            // 从配置中获取任务最大重试次数，默认为1
            int taskMaxRetryTimes = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES, 1);

            // 从配置中获取任务重试间隔时间，默认为10秒
            long taskRetryIntervalInMsec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC, 10000);

            // 从配置中获取任务最大等待时间，默认为60秒
            long taskMaxWaitInMsec = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC, 60000);

            // 获取任务配置列表
            List<Configuration> taskConfigs = this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

            // 如果启用了调试日志，则输出任务组的配置信息
            if(LOG.isDebugEnabled()) {
                LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId,
                        JSON.toJSONString(taskConfigs));
            }

            // 获取当前任务组中的任务数量
            int taskCountInThisTaskGroup = taskConfigs.size();
            // 输出日志，记录任务组ID、通道数量和任务数量
            LOG.info(String.format(
                    "taskGroupId=[%d] start [%d] channels for [%d] tasks.",
                    this.taskGroupId, channelNumber, taskCountInThisTaskGroup));

            // 注册任务配置与容器通信
            this.containerCommunicator.registerCommunication(taskConfigs);

            // 构建任务配置的ID映射
            Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs);
            // 构建待运行任务列表
            List<Configuration> taskQueue = buildRemainTasks(taskConfigs);
            // 初始化失败任务执行器映射
            Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<Integer, TaskExecutor>();
            // 初始化正在运行的任务列表
            List<TaskExecutor> runTasks = new ArrayList<TaskExecutor>(channelNumber);
            // 初始化任务开始时间映射
            Map<Integer, Long> taskStartTimeMap = new HashMap<Integer, Long>();

            // 初始化最后一次报告的时间戳
            long lastReportTimeStamp = 0;
            // 初始化任务组与容器的通信对象
            Communication lastTaskGroupContainerCommunication = new Communication();

            while (true) {
            	//1.判断task状态
            	// 标记任务是否失败或被终止
            	boolean failedOrKilled = false;
            	// 获取所有通信对象的映射
            	Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();
            	// 遍历所有通信对象
            	for(Map.Entry<Integer, Communication> entry : communicationMap.entrySet()){
            		// 获取任务ID
            		Integer taskId = entry.getKey();
            		// 获取任务的通信对象
                    Communication taskCommunication = entry.getValue();
                    // 如果任务未完成，则跳过当前循环
                    if(!taskCommunication.isFinished()){
                        continue;
                    }
                    // 从运行中的任务中移除并获取任务执行器
                    TaskExecutor taskExecutor = removeTask(runTasks, taskId);

                    // 相应地从监控中移除任务
                    taskMonitor.removeTask(taskId);

                    // 处理任务失败情况
            		if(taskCommunication.getState() == State.FAILED){
                        // 将任务执行器放入失败映射中
                        taskFailedExecutorMap.put(taskId, taskExecutor);
            			// 检查任务是否支持故障恢复且重试次数未超过最大限制
            			if(taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetryTimes){
                            // 关闭老的执行器
                            taskExecutor.shutdown();
                            // 重置任务的通信状态
                            containerCommunicator.resetCommunication(taskId);
            				// 获取任务配置
            				Configuration taskConfig = taskConfigMap.get(taskId);
            				// 重新加入任务队列
            				taskQueue.add(taskConfig);
            			}else{
            				// 标记失败或终止
            				failedOrKilled = true;
                			// 跳出循环
                			break;
            			}
            		// 处理任务被终止的情况
            		}else if(taskCommunication.getState() == State.KILLED){
            			// 标记失败或终止
            			failedOrKilled = true;
            			// 跳出循环
            			break;
            		// 处理任务成功完成的情况
                    }else if(taskCommunication.getState() == State.SUCCEEDED){
                        // 获取任务开始时间
                        Long taskStartTime = taskStartTimeMap.get(taskId);
                        // 如果开始时间不为空
                        if(taskStartTime != null){
                            // 计算任务执行时间
                            Long usedTime = System.currentTimeMillis() - taskStartTime;
                            // 记录日志
                            LOG.info("taskGroup[{}] taskId[{}] is successed, used[{}]ms",
                                    this.taskGroupId, taskId, usedTime);
                            // 将执行时间转换为ns并记录性能数据
                            PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL,taskStartTime, usedTime * 1000L * 1000L);
                            // 移除任务的开始时间记录和配置记录
                            taskStartTimeMap.remove(taskId);
                            taskConfigMap.remove(taskId);
                        }
                    }
            	}

            	
                // 2.发现该taskGroup下taskExecutor的总状态失败则汇报错误
                if (failedOrKilled) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    throw DataXException.asDataXException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, lastTaskGroupContainerCommunication.getThrowable());
                }
                
                //3.有任务未执行，且正在运行的任务数小于最大通道限制
                Iterator<Configuration> iterator = taskQueue.iterator();
                // 当任务队列中的任务数量少于通道数时，继续添加任务
                while(iterator.hasNext() && runTasks.size() < channelNumber){
                    Configuration taskConfig = iterator.next();
                    Integer taskId = taskConfig.getInt(CoreConstant.TASK_ID);
                    int attemptCount = 1;
                    TaskExecutor lastExecutor = taskFailedExecutorMap.get(taskId);
                    // 如果找到上一次失败的任务执行器，设置重试次数并计算失败时间
                    if(lastExecutor!=null){
                        attemptCount = lastExecutor.getAttemptCount() + 1;
                        long now = System.currentTimeMillis();
                        long failedTime = lastExecutor.getTimeStamp();
                        // 如果距离上一次失败时间未达到重试间隔，跳过当前任务
                        if(now - failedTime < taskRetryIntervalInMsec){
                            continue;
                        }
                        // 如果上一次失败的任务仍未结束，并且等待时间超过最大等待时间，标记通信失败并抛出异常
                        if(!lastExecutor.isShutdown()){
                            if(now - failedTime > taskMaxWaitInMsec){
                                markCommunicationFailed(taskId);
                                reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                                throw DataXException.asDataXException(CommonErrorCode.WAIT_TIME_EXCEED, "task failover等待超时");
                            }else{
                                lastExecutor.shutdown(); // 再次尝试关闭
                                continue;
                            }
                        }else{
                            LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] has already shutdown",
                                    this.taskGroupId, taskId, lastExecutor.getAttemptCount());
                        }
                    }
                    // 根据重试次数克隆配置或直接使用当前配置
                    Configuration taskConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.clone() : taskConfig;
                    // 创建任务执行器并开始执行任务
                    TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
                    taskStartTimeMap.put(taskId, System.currentTimeMillis());
                    taskExecutor.doStart();

                    iterator.remove();

                    //书签 注册任务
                    //这个Java函数的功能是将一个任务执行器（taskExecutor）添加到一个集合中（runTasks）。
                    // 这个操作通常用于管理系统中的多个任务执行器，以便稍后可以遍历这个集合，并依次调用每个任务执行器来执行特定任务。这里的add方法是List接口中的一个方法，用于向List中添加元素。
                    runTasks.add(taskExecutor);
                    taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));

                    // 清除失败任务执行器映射，并记录任务启动日志
                    taskFailedExecutorMap.remove(taskId);
                    LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] is started",
                            this.taskGroupId, taskId, attemptCount);
                }

                //4.任务列表为空，executor已结束, 搜集状态为success--->成功
                if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && containerCommunicator.collectState() == State.SUCCEEDED) {
                	// 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    LOG.info("taskGroup[{}] completed it's tasks.", this.taskGroupId);
                    break;
                }

                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    lastReportTimeStamp = now;

                    //taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
                    for(TaskExecutor taskExecutor:runTasks){
                        taskMonitor.report(taskExecutor.getTaskId(),this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
                    }

                }

                Thread.sleep(sleepIntervalInMillSec);
            }

            //6.最后还要汇报一次
            reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);


        } catch (Throwable e) {
            Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            this.containerCommunicator.report(nowTaskGroupContainerCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }finally {
            if(!PerfTrace.getInstance().isJob()){
                //最后打印cpu的平均消耗，GC的统计
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.info(vmInfo.totalString());
                }

                LOG.info(PerfTrace.getInstance().summarizeNoException());
            }
        }
    }
    
    private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations){
    	Map<Integer, Configuration> map = new HashMap<Integer, Configuration>();
    	for(Configuration taskConfig : configurations){
        	int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
        	map.put(taskId, taskConfig);
    	}
    	return map;
    }

    private List<Configuration> buildRemainTasks(List<Configuration> configurations){
    	List<Configuration> remainTasks = new LinkedList<Configuration>();
    	for(Configuration taskConfig : configurations){
    		remainTasks.add(taskConfig);
    	}
    	return remainTasks;
    }
    
    private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId){
    	Iterator<TaskExecutor> iterator = taskList.iterator();
    	while(iterator.hasNext()){
    		TaskExecutor taskExecutor = iterator.next();
    		if(taskExecutor.getTaskId() == taskId){
    			iterator.remove();
    			return taskExecutor;
    		}
    	}
    	return null;
    }
    
    private boolean isAllTaskDone(List<TaskExecutor> taskList){
    	for(TaskExecutor taskExecutor : taskList){
    		if(!taskExecutor.isTaskFinished()){
    			return false;
    		}
    	}
    	return true;
    }

    private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, int taskCount){
        Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication, taskCount);
        this.containerCommunicator.report(reportCommunication);
        return reportCommunication;
    }

    private void markCommunicationFailed(Integer taskId){
        Communication communication = containerCommunicator.getCommunication(taskId);
        communication.setState(State.FAILED);
    }

    /**
     * TaskExecutor是一个完整task的执行器
     * 其中包括1：1的reader和writer
     */
    class TaskExecutor {
        private Configuration taskConfig;

        private int taskId;

        private int attemptCount;

        private Channel channel;

        private Thread readerThread;

        private Thread writerThread;
        
        private ReaderRunner readerRunner;
        
        private WriterRunner writerRunner;

        /**
         * 该处的taskCommunication在多处用到：
         * 1. channel
         * 2. readerRunner和writerRunner
         * 3. reader和writer的taskPluginCollector
         */
        private Communication taskCommunication;

        /**
         * 构造一个新的TaskExecutor对象
         *
         * @param taskConf 任务的配置信息，用于初始化和配置任务执行环境
         * @param attemptCount 任务的尝试执行次数，用于管理任务重试逻辑
         */
        public TaskExecutor(Configuration taskConf, int attemptCount) {
            // 获取该taskExecutor的配置
            this.taskConfig = taskConf;
            Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // 得到taskId
            this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
            this.attemptCount = attemptCount;

            /**
             * 由taskId得到该taskExecutor的Communication
             * 要传给readerRunner和writerRunner，同时要传给channel作统计用
             */
            this.taskCommunication = containerCommunicator
                    .getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    String.format("taskId[%d]的Communication没有注册过", taskId));
            this.channel = ClassUtil.instantiate(channelClazz,
                    Channel.class, configuration);
            this.channel.setCommunication(this.taskCommunication);

            /**
             * 获取transformer的参数
             */

            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

            /**
             * 生成writerThread
             */
            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
            this.writerThread = new Thread(writerRunner,
                    String.format("%d-%d-%d-writer",
                            jobId, taskGroupId, this.taskId));
            //通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
            this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.WRITER, this.taskConfig.getString(
                            CoreConstant.JOB_WRITER_NAME)));

            /**
             * 生成readerThread
             */
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER,transformerInfoExecs);
            this.readerThread = new Thread(readerRunner,
                    String.format("%d-%d-%d-reader",
                            jobId, taskGroupId, this.taskId));
            /**
             * 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
             */
            this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.READER, this.taskConfig.getString(
                            CoreConstant.JOB_READER_NAME)));
        }

        public void doStart() {
            this.writerThread.start();

            // reader没有起来，writer不可能结束
            if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

            this.readerThread.start();

            // 这里reader可能很快结束
            if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

        }


        private AbstractRunner generateRunner(PluginType pluginType) {
            return generateRunner(pluginType, null);
        }

        /**
         * 根据插件类型生成相应的Runner实例
         *
         * @param pluginType 插件类型，如READER、WRITER等
         * @param transformerInfoExecs 转换器执行信息列表，用于存在转换器时的数据交换
         * @return AbstractRunner 返回生成的Runner实例
         *
         * 根据提供的插件类型和配置信息，实例化并配置相应的Runner。
         * 这包括加载Runner类、设置Job配置、初始化与插件相关的collector、
         * 配置记录发送和接收对象，以及设置Runner的通信和分组/任务ID信息。
         *
         * 对于不同类型的插件（如READER和WRITER），处理逻辑会有所不同，
         * 包括使用的配置参数不同，以及关联的插件collector实例化方式等。
         *
         * 如果插件类型不属于已知的Runner类型，则抛出异常。
         */
        private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs) {
            AbstractRunner newRunner = null;
            TaskPluginCollector pluginCollector;

            switch (pluginType) {
                case READER:
                    // 加载并配置Reader类型的Runner
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
                    newRunner.setJobConf(this.taskConfig.getConfiguration(
                            CoreConstant.JOB_READER_PARAMETER));

                    // 实例化处理脏数据和任务通信的collector
                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.READER);

                    RecordSender recordSender;
                    // 根据是否存在转换器，选择合适的记录发送器
                    if (transformerInfoExecs != null && transformerInfoExecs.size() > 0) {
                        recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId, this.channel,this.taskCommunication ,pluginCollector, transformerInfoExecs);
                    } else {
                        recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
                    }

                    // 设置ReaderRunner的记录发送器
                    ((ReaderRunner) newRunner).setRecordSender(recordSender);

                    // 设置taskPlugin的collector，用来处理脏数据和job/task通信
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                case WRITER:
                    // 加载并配置Writer类型的Runner
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
                    newRunner.setJobConf(this.taskConfig
                            .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

                    // 实例化处理脏数据和任务通信的collector
                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.WRITER);
                    // 设置WriterRunner的记录接收器
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
                            this.channel, pluginCollector));
                    // 设置taskPlugin的collector，用来处理脏数据和job/task通信
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                default:
                    // 对于未知插件类型，抛出异常
                    throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
            }

            // 设置Runner的分组ID、任务ID和通信实例
            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        // 检查任务是否结束
        private boolean isTaskFinished() {
            // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
            if (readerThread.isAlive() || writerThread.isAlive()) {
                return false;
            }

            if(taskCommunication==null || !taskCommunication.isFinished()){
        		return false;
        	}

            return true;
        }
        
        private int getTaskId(){
        	return taskId;
        }

        private long getTimeStamp(){
            return taskCommunication.getTimestamp();
        }

        private int getAttemptCount(){
            return attemptCount;
        }
        
        private boolean supportFailOver(){
        	return writerRunner.supportFailOver();
        }

        private void shutdown(){
            writerRunner.shutdown();
            readerRunner.shutdown();
            if(writerThread.isAlive()){
                writerThread.interrupt();
            }
            if(readerThread.isAlive()){
                readerThread.interrupt();
            }
        }

        private boolean isShutdown(){
            return !readerThread.isAlive() && !writerThread.isAlive();
        }
    }
}
