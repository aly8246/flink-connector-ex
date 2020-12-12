package com.github.aly8246.format.input;

import com.github.aly8246.option.JdbcOption;
import com.github.aly8246.option.JdbcSourceSinkContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.sql.*;

/**
 * 从数据库读取数据
 */
public class JdbcInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {
    private final JdbcSourceSinkContext context;

    //完整的查询sql
    private String sourceSql;

    private volatile Boolean hasNext = true;
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;
    private transient ResultSet resultSet;

    public JdbcInputFormat(JdbcSourceSinkContext context) {
        this.context = context;
    }


    @Override
    public InputSplit[] createInputSplits(int ii) {
        GenericInputSplit[] ret = new GenericInputSplit[ii];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) {
        try {
            if (inputSplit.getSplitNumber() == 1) {
                this.resultSet = this.preparedStatement.executeQuery();
                this.hasNext = this.resultSet.next();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 是否还有下一行数据
     */
    @Override
    public boolean reachedEnd() {
        //没有开始查询数据，不再有下一行数据了
        if (this.resultSet == null) return true;

        //判断resultSet是否还有下一行数据
        return !this.hasNext;
    }

    /**
     * 读取下一行数据
     */
    @Override
    public Row nextRecord(Row row) {
        if (!hasNext) {
            return null;
        }

        try {
            //读取已经获得的row
            for (int i = 0; i < row.getArity(); i++) {
                row.setField(i, resultSet.getObject(i + 1));
            }
            //标记下一行
            this.hasNext = resultSet.next();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return row;
    }

    @Override
    public void close() {
        try {
            if (this.resultSet != null)
                this.resultSet.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return this.context.getTableSchema().toRowType();
    }

    /**
     * 获取统计
     */
    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
        System.out.println("baseStatistics = " + baseStatistics);
        return baseStatistics;
    }

    @Override
    public void configure(Configuration configuration) {
        //do nothing here
    }


    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
        //提取sql
        ConditionResolver conditionResolver = new ConditionResolver(t.getTaskNameWithSubtasks());
        //提取条件语句
        String conditionSql = conditionResolver.extractWhereSql();
        //提取要查询的字段
        String[] selectFields = conditionResolver.extractSelectFields();

        //创建select的sql语句
        String selectFromStatement = this.context.selectStatement(selectFields);

        //拼接where条件，得到sql
        this.sourceSql = selectFromStatement + conditionSql;
    }


    /**
     * 开始从数据库加载数据
     */
    @Override
    public void openInputFormat() {
        this.connection = this.context.openConnection();
        try {
            //预编译stmt
            this.preparedStatement = this.connection.prepareStatement(this.sourceSql);

            //设置stmt一次fetch多少
            this.preparedStatement.setFetchSize(this.context.getOption().getFetchSize().intValue());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * 关闭读取数据
     */
    @Override
    public void closeInputFormat() {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        if (this.preparedStatement != null) {
            try {
                this.preparedStatement.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}
