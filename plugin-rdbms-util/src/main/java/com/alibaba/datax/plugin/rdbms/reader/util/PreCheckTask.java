package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.druid.sql.parser.ParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by judy.lt on 2015/6/4.
 */
public class PreCheckTask implements Callable<Boolean>{
    private static final Logger LOG = LoggerFactory.getLogger(PreCheckTask.class);
    private String userName;
    private String password;
    private String splitPkId;
    private Configuration connection;
    private DataBaseType dataBaseType;

    public PreCheckTask(String userName,
                        String password,
                        Configuration connection,
                        DataBaseType dataBaseType,
                        String splitPkId){
        this.connection = connection;
        this.userName=userName;
        this.password=password;
        this.dataBaseType = dataBaseType;
        this.splitPkId = splitPkId;
    }

    @Override
    /**
     * 执行数据库调用操作
     * 该方法主要负责连接数据库，验证查询SQL和分割主键SQL的有效性，以及执行初步的查询操作
     * 它体现了代码的高内聚性，包含了数据库连接、SQL验证、异常处理等多方面的功能
     *
     * @return Boolean 表示调用是否成功的标志
     * @throws DataXException 如果在调用过程中发生了不可恢复的错误，则抛出此异常
     */
    public Boolean call() throws DataXException {
        // 从连接配置中获取数据库URL
        String jdbcUrl = this.connection.getString(Key.JDBC_URL);
        // 获取查询SQL列表
        List<Object> querySqls = this.connection.getList(Key.QUERY_SQL, Object.class);
        // 获取用于分割主键的SQL列表
        List<Object> splitPkSqls = this.connection.getList(Key.SPLIT_PK_SQL, Object.class);
        // 获取表名列表，用于匹配查询SQL和分割主键SQL
        List<Object> tables = this.connection.getList(Key.TABLE,Object.class);

        // 根据数据库类型和URL建立数据库连接
        Connection conn = DBUtil.getConnectionWithoutRetry(this.dataBaseType, jdbcUrl,
                this.userName, password);

        // 初始化fetchSize大小，根据不同数据库类型设置不同值
        int fetchSize = 1;
        if(DataBaseType.MySql.equals(dataBaseType) || DataBaseType.DRDS.equals(dataBaseType)) {
            fetchSize = Integer.MIN_VALUE;
        }

        try{
            // 遍历查询SQL列表，执行SQL验证和初步查询操作
            for (int i=0;i<querySqls.size();i++) {

                // 初始化分割主键SQL
                String splitPkSql = null;
                // 获取当前查询SQL
                String querySql = querySqls.get(i).toString();

                // 初始化表名，如果列表不为空则获取对应表名
                String table = null;
                if (tables != null && !tables.isEmpty()) {
                    table = tables.get(i).toString();
                }

                /*验证查询SQL的有效性*/
                ResultSet rs = null;
                try {
                    // 验证SQL语法
                    DBUtil.sqlValid(querySql,dataBaseType);
                    // 执行查询，如果为第一次查询
                    if(i == 0) {
                        rs = DBUtil.query(conn, querySql, fetchSize);
                    }
                } catch (ParserException e) {
                    // 如果发生解析异常，抛出自定义异常
                    throw RdbmsException.asSqlParserException(this.dataBaseType, e, querySql);
                } catch (Exception e) {
                    // 如果发生其他异常，也抛出自定义异常
                    throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, userName);
                } finally {
                    // 无论如何，关闭结果集资源
                    DBUtil.closeDBResources(rs, null, null);
                }

                /*验证分割主键SQL的有效性*/
                try{
                    // 如果分割主键SQL列表不为空，则获取当前的分割主键SQL并验证其有效性
                    if (splitPkSqls != null && !splitPkSqls.isEmpty()) {
                        splitPkSql = splitPkSqls.get(i).toString();
                        DBUtil.sqlValid(splitPkSql,dataBaseType);
                        // 如果为第一次执行，进行预检查
                        if(i == 0) {
                            SingleTableSplitUtil.precheckSplitPk(conn, splitPkSql, fetchSize, table, userName);
                        }
                    }
                } catch (ParserException e) {
                    // 如果发生解析异常，抛出自定义异常
                    throw RdbmsException.asSqlParserException(this.dataBaseType, e, splitPkSql);
                } catch (DataXException e) {
                    // 如果发生DataX异常，直接抛出
                    throw e;
                } catch (Exception e) {
                    // 如果发生其他异常，抛出自定义异常
                    throw RdbmsException.asSplitPKException(this.dataBaseType, e, splitPkSql,this.splitPkId.trim());
                }
            }
        } finally {
            // 无论如何，关闭数据库连接
            DBUtil.closeDBResources(null, conn);
        }

        // 返回调用成功标志
        return true;
    }

}
