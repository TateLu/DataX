package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public class SingleTableSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(SingleTableSplitUtil.class);

    public static DataBaseType DATABASE_TYPE;

    private SingleTableSplitUtil() {
    }

    /**
     * 对单表进行切分以生成配置列表
     *
     * @param configuration 数据库配置信息，包含切分主键、表名、列名等
     * @param adviceNum 建议的切分数量
     * @return 返回切分后的配置列表
     *
     * 此方法根据提供的数据库配置和切分数量，对单个表进行切分操作，生成用于查询的SQL并返回相应的配置列表
     * 主要功能包括：
     * 1. 根据切分主键类型生成切分范围
     * 2. 根据数据库类型选择合适的切分逻辑（如Oracle数据库的特殊处理）
     * 3. 处理切分主键为空的情况
     * 4. 生成切分后的SQL查询语句并添加到配置列表中
     */
    public static List<Configuration> splitSingleTable(
            Configuration configuration, int adviceNum) {
        List<Configuration> pluginParams = new ArrayList<Configuration>();
        List<String> rangeList;
        String splitPkName = configuration.getString(Key.SPLIT_PK);
        String column = configuration.getString(Key.COLUMN);
        String table = configuration.getString(Key.TABLE);
        String where = configuration.getString(Key.WHERE, null);
        boolean hasWhere = StringUtils.isNotBlank(where);

        // 以下为Oracle数据库的特殊处理逻辑
        if (DATABASE_TYPE == DataBaseType.Oracle) {
            rangeList = genSplitSqlForOracle(splitPkName, table, where,
                    configuration, adviceNum);
        } else {
            Pair<Object, Object> minMaxPK = getPkRange(configuration);
            if (null == minMaxPK) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                        "根据切分主键切分表失败. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
            }

            configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
            if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
                pluginParams.add(configuration);
                return pluginParams;
            }

            boolean isStringType = Constant.PK_TYPE_STRING.equals(configuration.getString(Constant.PK_TYPE));
            boolean isLongType = Constant.PK_TYPE_LONG.equals(configuration.getString(Constant.PK_TYPE));

            // 根据切分主键的类型进行切分逻辑处理
            if (isStringType) {
                // 根据最小和最大主键值分割范围并包装成易于处理的格式
                // 这对于大规模数据处理任务，如数据导入或查询分区特别有用
                rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                        String.valueOf(minMaxPK.getLeft()), // 将最小主键值转换为字符串，以便进行范围比较和分割
                        String.valueOf(minMaxPK.getRight()), // 将最大主键值转换为字符串，以便进行范围比较和分割
                        adviceNum, // 提供一个建议数，指示希望获得的分割区间数量
                        splitPkName, // 主键字段的名称，用于在分割时准确地引用数据库中的字段
                        "'", // 引用符号，用于正确构造SQL语句，确保主键范围的正确解析
                        DATABASE_TYPE); // 数据库类型，确保分割逻辑与特定数据库的特性相匹配，如SQL语法差异
            } else if (isLongType) {
                rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                        new BigInteger(minMaxPK.getLeft().toString()),
                        new BigInteger(minMaxPK.getRight().toString()),
                        adviceNum, splitPkName);
            } else {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                        "您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
            }
        }
        String tempQuerySql;
        List<String> allQuerySql = new ArrayList<String>();

        // 根据切分范围生成SQL查询语句
        if (null != rangeList && !rangeList.isEmpty()) {
            // 遍历范围列表，针对每个范围生成特定的查询SQL，并配置查询条件
            for (String range : rangeList) {
                // 通过克隆现有配置来创建临时配置对象，以避免修改原始配置
                Configuration tempConfig = configuration.clone();

                // 根据传入的列、表和where条件构建查询SQL，然后根据是否已存在where条件添加当前范围的条件
                tempQuerySql = buildQuerySql(column, table, where)
                        + (hasWhere ? " and " : " where ") + range;

                // 将生成的查询SQL添加到全部查询SQL列表中
                allQuerySql.add(tempQuerySql);
                // 将临时配置对象和对应的查询SQL关联起来
                tempConfig.set(Key.QUERY_SQL, tempQuerySql);
                // 根据是否存在where条件，设置适当的查询条件
                tempConfig.set(Key.WHERE, (hasWhere ? ("(" + where + ") and") : "") + range);
                // 将配置了查询条件的临时配置对象添加到插件参数列表中
                pluginParams.add(tempConfig);
            }
        } else {
            // 克隆当前配置，以准备进行下一个查询的配置
            Configuration tempConfig = configuration.clone();
            // 构建查询SQL语句，此处添加了where子句以筛选非空的splitPkName字段
            tempQuerySql = buildQuerySql(column, table, where)
                    + (hasWhere ? " and " : " where ")
                    + String.format(" %s IS NOT NULL", splitPkName);
            // 将构建的查询SQL语句添加到集合中
            allQuerySql.add(tempQuerySql);
            // 设置临时配置的查询SQL
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            // 根据是否有现有的where子句来构建新的where子句，并设置到临时配置中
            tempConfig.set(Key.WHERE, (hasWhere ? "(" + where + ") and" : "") + String.format(" %s IS NOT NULL", splitPkName));
            // 将临时配置添加到插件参数列表中，以供后续处理
            pluginParams.add(tempConfig);
        }


        // 处理切分主键为空的情况
        Configuration tempConfig = configuration.clone();
        tempQuerySql = buildQuerySql(column, table, where)
                + (hasWhere ? " and " : " where ")
                + String.format(" %s IS NULL", splitPkName);

        allQuerySql.add(tempQuerySql);

        LOG.info("After split(), allQuerySql=[\n{}\n].",
                StringUtils.join(allQuerySql, "\n"));

        tempConfig.set(Key.QUERY_SQL, tempQuerySql);
        tempConfig.set(Key.WHERE, (hasWhere ? "(" + where + ") and" : "") + String.format(" %s IS NULL", splitPkName));
        pluginParams.add(tempConfig);
        
        return pluginParams;
    }

    public static String buildQuerySql(String column, String table,
                                          String where) {
        String querySql;

        if (StringUtils.isBlank(where)) {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE_WITHOUT_WHERE,
                    column, table);
        } else {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE, column,
                    table, where);
        }

        return querySql;
    }

    @SuppressWarnings("resource")
    /**
     * 根据配置获取表的主键范围
     *
     * @param configuration 配置信息，用于获取连接信息和配置参数
     * @return 返回包含最小和最大主键值的Pair对象
     */
    private static Pair<Object, Object> getPkRange(Configuration configuration) {
        // 生成用于查询主键范围的SQL语句
        String pkRangeSQL = genPKRangeSQL(configuration);

        // 从配置中获取各项参数
        int fetchSize = configuration.getInt(Constant.FETCH_SIZE);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String table = configuration.getString(Key.TABLE);

        // 获取数据库连接
        Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcURL, username, password);
        // 检查并获取主键的最小和最大值
        Pair<Object, Object> minMaxPK = checkSplitPk(conn, pkRangeSQL, fetchSize, table, username, configuration);
        // 关闭数据库连接
        DBUtil.closeDBResources(null, null, conn);
        // 返回主键的最小和最大值
        return minMaxPK;
    }


    public static void precheckSplitPk(Connection conn, String pkRangeSQL, int fetchSize,
                                                       String table, String username) {
        Pair<Object, Object> minMaxPK = checkSplitPk(conn, pkRangeSQL, fetchSize, table, username, null);
        if (null == minMaxPK) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "根据切分主键切分表失败. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
        }
    }

    /**
     * 检测splitPk的配置是否正确。
     * configuration为null, 是precheck的逻辑，不需要回写PK_TYPE到configuration中
     *
     */
    /**
     * 检查并获取拆分主键的最小和最大值
     *
     * @param conn 数据库连接
     * @param pkRangeSQL 用于查询拆分主键范围的SQL语句
     * @param fetchSize 每次获取的结果集大小
     * @param table 表名
     * @param username 用户名
     * @param configuration 配置信息
     * @return 返回一个Pair对象，包含拆分主键的最小和最大值
     *
     * 该方法通过执行pkRangeSQL来查询拆分主键的范围，并根据结果集的类型（字符串或长整型）进行处理，
     * 同时更新配置信息中的拆分主键类型。如果查询过程中发生异常，会抛出相应的异常。
     */
    private static Pair<Object, Object> checkSplitPk(Connection conn, String pkRangeSQL, int fetchSize,  String table,
                                                     String username, Configuration configuration) {
        LOG.info("split pk [sql={}] is running... ", pkRangeSQL);
        ResultSet rs = null;
        Pair<Object, Object> minMaxPK = null;
        try {
            // 尝试执行PK范围查询并设置fetchSize，以优化大规模数据的检索
            try {
                rs = DBUtil.query(conn, pkRangeSQL, fetchSize);
            }catch (Exception e) {
                // 若查询过程中出现异常，抛出自定义异常，以便于错误追踪和处理
                throw RdbmsException.asQueryException(DATABASE_TYPE, e, pkRangeSQL, table, username);
            }
            // 获取结果集的元数据，用于后续的数据结构分析和处理
            ResultSetMetaData rsMetaData = rs.getMetaData();
            // 检查结果集元数据的主键类型是否有效
            if (isPKTypeValid(rsMetaData)) {

                // 判断结果集元数据的第一列类型是否为字符串类型
                if (isStringType(rsMetaData.getColumnType(1))) {
                    if(configuration != null) {
                        configuration
                                .set(Constant.PK_TYPE, Constant.PK_TYPE_STRING);
                    }
                    // 遍历结果集以获取最小和最大主键值
                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<Object, Object>(
                                rs.getString(1), rs.getString(2));
                    }
                } else if (isLongType(rsMetaData.getColumnType(1))) {
                    if(configuration != null) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_LONG);
                    }

                    // 遍历结果集以获取最小和最大主键值
                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<Object, Object>(
                                rs.getString(1), rs.getString(2));

                        // 对于Oracle数据库，验证主键值不包含'.', 因为DataX不支持这种类型
                        String minMax = rs.getString(1) + rs.getString(2);
                        if (StringUtils.contains(minMax, '.')) {
                            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                                    "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
                        }
                    }
                } else {
                    // 如果主键类型既不是字符串也不是长整型，抛出异常
                    throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                            "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
                }
            } else {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                        "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
            }
        } catch(DataXException e) {
            throw e;
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, "DataX尝试切分表发生错误. 请检查您的配置并作出修改.", e);
        } finally {
            DBUtil.closeDBResources(rs, null, null);
        }

        return minMaxPK;
    }


    /**
     * 检查主键类型是否有效
     * 该方法用于确定结果集元数据中的主键字段类型是否符合要求，主要是检查最小和最大列的类型，
     * 并判断其是否为长整型或字符串类型，以此来确定切分主键的可行性
     *
     * @param rsMetaData 结果集的元数据，用于获取列的类型信息
     * @return 如果主键类型有效，则返回true；否则返回false
     * @throws DataXException 如果获取主键类型过程中发生异常，则抛出自定义的DataXException
     */
    private static boolean isPKTypeValid(ResultSetMetaData rsMetaData) {
        boolean ret = false;
        try {
            // 获取第一列（最小列）的类型
            int minType = rsMetaData.getColumnType(1);
            // 获取第二列（最大列）的类型
            int maxType = rsMetaData.getColumnType(2);

            // 检查是否为长整型
            boolean isNumberType = isLongType(minType);
            // 检查是否为字符串类型
            boolean isStringType = isStringType(minType);

            // 如果最小和最大列的类型相同，并且至少为长整型或字符串类型之一，则认为主键类型有效
            if (minType == maxType && (isNumberType || isStringType)) {
                ret = true;
            }
        } catch (Exception e) {
            // 如果发生异常，抛出自定义异常，提示获取切分主键字段类型失败
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX获取切分主键(splitPk)字段类型失败. 该错误通常是系统底层异常导致. 请联系旺旺:askdatax或者DBA处理.");
        }
        return ret;
    }


    // warn: Types.NUMERIC is used for oracle! because oracle use NUMBER to
    // store INT, SMALLINT, INTEGER etc, and only oracle need to concern
    // Types.NUMERIC
    /**
     * 检查给定的类型是否属于长整型类别
     *
     * 此方法用于确定传入的数据库类型是否被视为长整型，包括BIGINT、INTEGER、SMALLINT和TINYINT
     * 根据不同的数据库类型，可能还将NUMERIC包括在内
     *
     * @param type 要检查的数据库类型，通常来自java.sql.Types
     * @return 如果给定类型是长整型类别，则返回true；否则返回false
     */
    private static boolean isLongType(int type) {
        // 初始化isValidLongType为false，后续根据type的值进行条件判断
        boolean isValidLongType = type == Types.BIGINT || type == Types.INTEGER
                || type == Types.SMALLINT || type == Types.TINYINT;

        // 根据全局数据库类型变量进一步判断，是否将NUMERIC也视为有效的长整型
        switch (SingleTableSplitUtil.DATABASE_TYPE) {
            // 对于Oracle和OceanBase数据库，NUMERIC也被认为是长整型
            case Oracle:
            case OceanBase:
                isValidLongType |= type == Types.NUMERIC;
                break;
            default:
                // 其他数据库类型不考虑NUMERIC为长整型
                break;
        }
        // 返回最终的判断结果
        return isValidLongType;
    }
    
    private static boolean isStringType(int type) {
        return type == Types.CHAR || type == Types.NCHAR
                || type == Types.VARCHAR || type == Types.LONGVARCHAR
                || type == Types.NVARCHAR;
    }

    private static String genPKRangeSQL(Configuration configuration) {

        String splitPK = configuration.getString(Key.SPLIT_PK).trim();
        String table = configuration.getString(Key.TABLE).trim();
        String where = configuration.getString(Key.WHERE, null);
        String obMode = configuration.getString("obCompatibilityMode");
        // OceanBase对SELECT MIN(%s),MAX(%s) FROM %s这条sql没有做查询改写，会进行全表扫描，在数据量的时候查询耗时很大甚至超时；
        // 所以对于OceanBase数据库，查询模板需要改写为分别查询最大值和最小值。这样可以提升查询数量级的性能。
        if (DATABASE_TYPE == DataBaseType.OceanBase && StringUtils.isNotEmpty(obMode)) {
            boolean isOracleMode = "ORACLE".equalsIgnoreCase(obMode);

            String minMaxTemplate = isOracleMode ? "select v2.id as min_a, v1.id as max_a from ("
                + "select * from (select %s as id from %s {0} order by id desc) where rownum =1 ) v1,"
                + "(select * from (select %s as id from %s order by id asc) where rownum =1 ) v2;" :
                "select v2.id as min_a, v1.id as max_a from (select %s as id from %s {0} order by id desc limit 1) v1,"
                    + "(select %s as id from %s order by id asc limit 1) v2;";

            String pkRangeSQL = String.format(minMaxTemplate, splitPK, table, splitPK, table);
            String whereString = StringUtils.isNotBlank(where) ? String.format("WHERE (%s AND %s IS NOT NULL)", where, splitPK) : EMPTY;
            pkRangeSQL = MessageFormat.format(pkRangeSQL, whereString);
            return pkRangeSQL;
        }
        return genPKSql(splitPK, table, where);
    }

    public static String genPKSql(String splitPK, String table, String where){

        String minMaxTemplate = "SELECT MIN(%s),MAX(%s) FROM %s";
        String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK,
                table);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL = String.format("%s WHERE (%s AND %s IS NOT NULL)",
                    pkRangeSQL, where, splitPK);
        }
        return pkRangeSQL;
    }
    
    /**
     * support Number and String split
     * */
    public static List<String> genSplitSqlForOracle(String splitPK,
            String table, String where, Configuration configuration,
            int adviceNum) {
        if (adviceNum < 1) {
            throw new IllegalArgumentException(String.format(
                    "切分份数不能小于1. 此处:adviceNum=[%s].", adviceNum));
        } else if (adviceNum == 1) {
            return null;
        }
        String whereSql = String.format("%s IS NOT NULL", splitPK);
        if (StringUtils.isNotBlank(where)) {
            whereSql = String.format(" WHERE (%s) AND (%s) ", whereSql, where);
        } else {
            whereSql = String.format(" WHERE (%s) ", whereSql);
        }
        Double percentage = configuration.getDouble(Key.SAMPLE_PERCENTAGE, 0.1);
        String sampleSqlTemplate = "SELECT * FROM ( SELECT %s FROM %s SAMPLE (%s) %s ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= %s ORDER by %s ASC";
        String splitSql = String.format(sampleSqlTemplate, splitPK, table,
                percentage, whereSql, adviceNum, splitPK);

        int fetchSize = configuration.getInt(Constant.FETCH_SIZE, 32);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcURL,
                username, password);
        LOG.info("split pk [sql={}] is running... ", splitSql);
        ResultSet rs = null;
        List<Pair<Object, Integer>> splitedRange = new ArrayList<Pair<Object, Integer>>();
        try {
            try {
                rs = DBUtil.query(conn, splitSql, fetchSize);
            } catch (Exception e) {
                throw RdbmsException.asQueryException(DATABASE_TYPE, e,
                        splitSql, table, username);
            }
            if (configuration != null) {
                configuration
                        .set(Constant.PK_TYPE, Constant.PK_TYPE_MONTECARLO);
            }
            ResultSetMetaData rsMetaData = rs.getMetaData();
            while (DBUtil.asyncResultSetNext(rs)) {
                ImmutablePair<Object, Integer> eachPoint = new ImmutablePair<Object, Integer>(
                        rs.getObject(1), rsMetaData.getColumnType(1));
                splitedRange.add(eachPoint);
            }
        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX尝试切分表发生错误. 请检查您的配置并作出修改.", e);
        } finally {
            DBUtil.closeDBResources(rs, null, null);
        }
        LOG.debug(JSON.toJSONString(splitedRange));
        List<String> rangeSql = new ArrayList<String>();
        int splitedRangeSize = splitedRange.size();
        // warn: splitedRangeSize may be 0 or 1，切分规则为IS NULL以及 IS NOT NULL
        // demo: Parameter rangeResult can not be null and its length can not <2. detail:rangeResult=[24999930].
        if (splitedRangeSize >= 2) {
            // warn: oracle Number is long type here
            if (isLongType(splitedRange.get(0).getRight())) {
                BigInteger[] integerPoints = new BigInteger[splitedRange.size()];
                for (int i = 0; i < splitedRangeSize; i++) {
                    integerPoints[i] = new BigInteger(splitedRange.get(i)
                            .getLeft().toString());
                }
                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(integerPoints,
                        splitPK));
                // its ok if splitedRangeSize is 1
                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(
                        integerPoints[0], integerPoints[splitedRangeSize - 1],
                        splitPK));
            } else if (isStringType(splitedRange.get(0).getRight())) {
                // warn: treated as string type
                String[] stringPoints = new String[splitedRange.size()];
                for (int i = 0; i < splitedRangeSize; i++) {
                    stringPoints[i] = new String(splitedRange.get(i).getLeft()
                            .toString());
                }
                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(stringPoints,
                        splitPK, "'", DATABASE_TYPE));
                // its ok if splitedRangeSize is 1
                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(
                        stringPoints[0], stringPoints[splitedRangeSize - 1],
                        splitPK, "'", DATABASE_TYPE));
            } else {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                                "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
            }
        }
        return rangeSql;
    }
}