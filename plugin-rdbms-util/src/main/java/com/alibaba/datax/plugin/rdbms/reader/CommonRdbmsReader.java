package com.alibaba.datax.plugin.rdbms.reader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.PreCheckTask;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CommonRdbmsReader {

    public static class Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        public Job(DataBaseType dataBaseType) {
            OriginalConfPretreatmentUtil.DATABASE_TYPE = dataBaseType;
            SingleTableSplitUtil.DATABASE_TYPE = dataBaseType;
        }

        public void init(Configuration originalConfig) {

            OriginalConfPretreatmentUtil.doPretreatment(originalConfig);

            LOG.debug("After job init(), job config now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        /**
         * 执行预检查，确保数据读取的正确性和有效性
         *
         * @param originalConfig 原始配置信息，包含数据读取的各项参数
         * @param dataBaseType 数据库类型，用于区分不同的数据库以执行相应的预检查逻辑
         */
        public void preCheck(Configuration originalConfig, DataBaseType dataBaseType) {
            // 根据原始配置执行分割预检查，确保每个表都有读权限，且querySql和split Key正确
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            // 提取分割主键，用于后续处理
            String splitPK = queryConf.getString(Key.SPLIT_PK);
            // 提取连接列表，用于并行执行预检查任务
            List<Object> connList = queryConf.getList(Constant.CONN_MARK, Object.class);
            // 提取用户名和密码，用于数据库连接
            String username = queryConf.getString(Key.USERNAME);
            String password = queryConf.getString(Key.PASSWORD);

            // 根据连接列表的大小，创建固定大小的线程池
            ExecutorService exec;
            if (connList.size() < 10){
                exec = Executors.newFixedThreadPool(connList.size());
            }else{
                exec = Executors.newFixedThreadPool(10);
            }

            // 初始化预检查任务列表
            Collection<PreCheckTask> taskList = new ArrayList<PreCheckTask>();
            for (int i = 0, len = connList.size(); i < len; i++){
                // 从连接配置中创建新的配置对象
                Configuration connConf = Configuration.from(connList.get(i).toString());
                // 创建预检查任务，包含用户名、密码、连接配置、数据库类型和分割主键
                PreCheckTask t = new PreCheckTask(username,password,connConf,dataBaseType,splitPK);
                taskList.add(t);
            }

            // 初始化保存所有任务执行结果的列表
            List<Future<Boolean>> results = Lists.newArrayList();
            try {
                // 执行所有预检查任务，并获取结果
                results = exec.invokeAll(taskList);
            } catch (InterruptedException e) {
                // 若执行过程中被中断，保留中断状态
                Thread.currentThread().interrupt();
            }

            // 遍历所有任务的执行结果
            for (Future<Boolean> result : results){
                try {
                    // 获取任务的执行结果，若结果为false或有异常将抛出
                    result.get();
                } catch (ExecutionException e) {
                    // 若任务执行异常，抛出自定义异常
                    DataXException de = (DataXException) e.getCause();
                    throw de;
                }catch (InterruptedException e) {
                    // 若获取结果过程中被中断，保留中断状态
                    Thread.currentThread().interrupt();
                }
            }
            // 关闭线程池，释放资源
            exec.shutdownNow();
        }


        public List<Configuration> split(Configuration originalConfig,
                                         int adviceNumber) {
            return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }

    }

    public static class Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private DataBaseType dataBaseType;
        private int taskGroupId = -1;
        private int taskId=-1;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private String basicMsg;

        public Task(DataBaseType dataBaseType) {
            this(dataBaseType, -1, -1);
        }

        public Task(DataBaseType dataBaseType,int taskGropuId, int taskId) {
            this.dataBaseType = dataBaseType;
            this.taskGroupId = taskGropuId;
            this.taskId = taskId;
        }

        public void init(Configuration readerSliceConfig) {

			/* for database connection */

            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);

            //ob10的处理
            if (this.jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING) && this.dataBaseType == DataBaseType.MySql) {
                String[] ss = this.jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
                }
                LOG.info("this is ob1_0 jdbc url.");
                this.username = ss[1].trim() +":"+this.username;
                this.jdbcUrl = ss[2];
                LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
            }

            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);

        }
        //书签 读取数据 mysql
        /**
         * 开始读取数据
         *
         * @param readerSliceConfig 读取切片的配置，用于定制化读取过程
         * @param recordSender 记录发送器，用于发送读取到的数据
         * @param taskPluginCollector 插件收集器，用于监控任务执行情况
         * @param fetchSize 每次读取的数据量，用于控制读取效率和内存使用
         */
        public void startRead(Configuration readerSliceConfig,
                              RecordSender recordSender,
                              TaskPluginCollector taskPluginCollector, int fetchSize) {
            // 核心逻辑：根据传入的配置和参数，初始化读取操作，设置记录发送器和插件收集器
            // 控制每次读取的数据量，以提高效率和减少内存占用

            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
            String table = readerSliceConfig.getString(Key.TABLE);

            PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

            LOG.info("Begin to read record by Sql: [{}\n] {}.",
                    querySql, basicMsg);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();
            //获取jdbc链接
            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
                    username, password);

            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
                    this.dataBaseType, basicMsg);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                // 执行查询操作并获取结果集
                // 这里使用了DBUtil工具类的query方法来执行SQL查询，该方法封装了查询过程，简化了数据库操作
                // 参数：
                // - conn：数据库连接对象，表示与数据库的连接
                // - querySql：字符串类型的SQL查询语句，用于指定要从数据库中检索的数据
                // - fetchSize：表示每次从数据库中获取的行数，用于优化内存使用和查询性能
                // 返回值：
                // 方法返回一个结果集（ResultSet对象），包含查询到的所有数据
                rs = DBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                //这个统计干净的result_Next时间
                PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();

                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    //解析一条记录
                    this.transportOneRecord(recordSender, rs,
                            metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                //目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                LOG.info("Finished read record by Sql: [{}\n] {}.",
                        querySql, basicMsg);

            }catch (Exception e) {
                throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }
        
        /**
         * 传输单条记录方法
         * 该方法根据结果集（ResultSet）和元数据（ResultSetMetaData）构建一条记录（Record），
         * 并通过RecordSender发送该记录进行写入，最后返回构建的记录
         *
         * @param recordSender 用于发送记录的发送器
         * @param rs 结果集，包含要传输的数据
         * @param metaData 结果集的元数据，用于理解结果集中的数据类型和结构
         * @param columnNumber 结果集中列的数量，用于遍历和处理数据列
         * @param mandatoryEncoding 强制编码，确保数据传输时的编码一致性
         * @param taskPluginCollector 任务插件收集器，用于收集任务执行中的相关信息
         * @return 返回根据结果集构建的记录
         */
        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                TaskPluginCollector taskPluginCollector) {
            // 根据参数构建单条记录
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
            // 将构建的记录发送到写入器进行写入
            recordSender.sendToWriter(record);
            // 返回构建的记录
            return record;
        }
        /**
         * 构建并返回一个Record对象
         * 该方法根据提供的ResultSet对象中的数据、结果集元数据和相关参数，创建一个Record实例并用以封装这些信息
         * 主要用于将数据库查询结果的一行转换为Record对象，以便后续处理
         *
         * @param recordSender 用于发送记录的发送者，此处未直接用于构建Record，但可能用于记录关联信息或配置
         * @param rs 结果集对象，代表数据库查询的结果，将从此结果集中提取一行数据来填充Record对象
         * @param metaData 结果集的元数据对象，提供关于结果集列的信息，如列名、列类型等
         * @param columnNumber 列数，指示结果集中列的数量，用于处理变长的记录结构
         * @param mandatoryEncoding 强制编码，指定处理文本数据时必须使用的编码方式，确保数据编码的一致性
         * @param taskPluginCollector 任务插件收集器，用于收集或报告任务执行过程中的相关信息或问题，增强任务执行的可监控性和可维护性
         * @return 返回一个填充了ResultSet中一行数据的Record对象，作为后续处理的数据单元
         */
        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                     TaskPluginCollector taskPluginCollector) {
        	Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        String rawData;
                        if(StringUtils.isBlank(mandatoryEncoding)){
                            rawData = rs.getString(i);
                        }else{
                            rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : 
                                rs.getBytes(i)), mandatoryEncoding);
                        }
                        record.addColumn(new StringColumn(rawData));
                        break;

                    case Types.CLOB:
                    case Types.NCLOB:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;

                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;

                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.TIME:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;

                    // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                    case Types.DATE:
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;

                    case Types.TIMESTAMP:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;

                    // warn: bit(1) -> Types.BIT 可使用BoolColumn
                    // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;

                    case Types.NULL:
                        String stringData = null;
                        if(rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }
                        record.addColumn(new StringColumn(stringData));
                        break;

                    default:
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.UNSUPPORTED_TYPE,
                                        String.format(
                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                metaData.getColumnName(i),
                                                metaData.getColumnType(i),
                                                metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }
    }

}
