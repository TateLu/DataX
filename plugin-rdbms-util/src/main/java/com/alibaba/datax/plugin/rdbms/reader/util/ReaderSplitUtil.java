package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * ReaderSplitUtil类提供了处理读取任务分割的工具和方法。
 * 这个类的主要作用是将大型阅读任务分割为更小、更可管理的片段，
 * 以便于并行处理或分布式处理。这对于大数据处理场景尤其重要，
 * 例如，分割一个大文件到多个小文件，每个小文件可以被单独处理。
 */
public final class ReaderSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderSplitUtil.class);

    /**
     * 执行配置分割方法
     * 根据原始配置和建议数量来分割配置，以适应DataX并行任务的需求
     *
     * @param originalSliceConfig 原始配置项，包含数据库连接信息、切分列、WHERE条件等
     * @param adviceNumber 建议的分割数量，即DataX并行任务的数量
     * @return 返回分割后的Configuration列表，每个Configuration代表一个子任务的配置
     */
    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber) {
        // 判断是否是表模式，即是否需要对多个表进行切分
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();
        // 单表应该切分的份数初始化为-1，表示未设置或不适用
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            // adviceNumber这里是channel数量大小, 即datax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
            // 计算每个表应该切分的份数
            eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber, originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
        }

        // 获取切分列名
        String column = originalSliceConfig.getString(Key.COLUMN);
        // 获取WHERE条件，可能为null
        String where = originalSliceConfig.getString(Key.WHERE, null);

        // 获取连接信息列表
        List<Object> conns = originalSliceConfig.getList(Constant.CONN_MARK, Object.class);

        // 初始化分割后的配置列表
        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        // 遍历连接列表，为每个连接生成切分配置
        for (int i = 0, len = conns.size(); i < len; i++) {
            // 克隆原始切片配置以避免修改原始配置
            Configuration sliceConfig = originalSliceConfig.clone();

            // 从当前连接配置中获取JDBC URL
            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            // 提取jdbcUrl中的ip/port用于资源标记，以支持有意义的shuffle操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            // 移除连接标记配置
            sliceConfig.remove(Constant.CONN_MARK);

            // 临时切片配置
            Configuration tempSlice;

            // 如果是表模式
            if (isTableMode) {
                // 从配置中获取表列表
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                // 确保表列表不为空
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                // 获取切分主键配置
                String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);

                // 判断是否需要对表进行切分
                boolean needSplitTable = eachTableShouldSplittedNumber > 1
                        && StringUtils.isNotBlank(splitPk);
                if (needSplitTable) {
                    // 如果是单表，调整切分数量
                    if (tables.size() == 1) {
                        // 根据切分因子调整切分数量
                        Integer splitFactor = originalSliceConfig.getInt(Key.SPLIT_FACTOR, Constant.SPLIT_FACTOR);
                        eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * splitFactor;
                    }
                    // 对每个表进行切分
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);

                        // 切分单个表
                        List<Configuration> splittedSlices = SingleTableSplitUtil
                                .splitSingleTable(tempSlice, eachTableShouldSplittedNumber);
                        splittedConfigs.addAll(splittedSlices);
                    }
                } else {
                    // 对每个表生成查询语句
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column);
                        tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                        splittedConfigs.add(tempSlice);
                    }
                }
            } else {
                // 如果是查询语句模式
                List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);
                for (String querySql : sqls) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.QUERY_SQL, querySql);
                    splittedConfigs.add(tempSlice);
                }
            }
        }

        // 返回分割后的配置项列表
        return splittedConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig) {
        Configuration queryConfig = originalSliceConfig.clone();
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();

        String splitPK = originalSliceConfig.getString(Key.SPLIT_PK);
        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = queryConfig.getList(Constant.CONN_MARK, Object.class);

        for (int i = 0, len = conns.size(); i < len; i++){
            Configuration connConf = Configuration.from(conns.get(i).toString());
            List<String> querys = new ArrayList<String>();
            List<String> splitPkQuerys = new ArrayList<String>();
            String connPath = String.format("connection[%d]",i);
            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
                for (String table : tables) {
                    querys.add(SingleTableSplitUtil.buildQuerySql(column,table,where));
                    if (splitPK != null && !splitPK.isEmpty()){
                        splitPkQuerys.add(SingleTableSplitUtil.genPKSql(splitPK.trim(),table,where));
                    }
                }
                if (!splitPkQuerys.isEmpty()){
                    connConf.set(Key.SPLIT_PK_SQL,splitPkQuerys);
                }
                connConf.set(Key.QUERY_SQL,querys);
                queryConfig.set(connPath,connConf);
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL,
                        String.class);
                for (String querySql : sqls) {
                    querys.add(querySql);
                }
                connConf.set(Key.QUERY_SQL,querys);
                queryConfig.set(connPath,connConf);
            }
        }
        return queryConfig;
    }

    /**
     * 计算每个表应分配的拆分数量
     * 该方法用于根据建议的拆分总数和表的数量，计算每个表应该被拆分的次数
     * 在数据迁移过程中，为了平衡每个表的开销，这个方法确保了每个表的拆分次数尽可能均匀
     *
     * @param adviceNumber 总的拆分建议数量，表示希望进行的拆分总数
     * @param tableNumber 表的数量，表示需要拆分的表的数量
     * @return 每个表应分配的拆分数量，返回每个表需要进行的拆分次数
     */
    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
                                                              int tableNumber) {
        // 计算每个表平均应分配的拆分数量，使用浮点数进行精确计算
        double tempNum = 1.0 * adviceNumber / tableNumber;

        // 使用Math.ceil将计算结果向上取整，以确保拆分次数的分配不会小于实际需要的次数
        return (int) Math.ceil(tempNum);
    }


}
