package com.github.aly8246.format.input;

import com.github.aly8246.option.JdbcSourceSinkContext;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;

public class JdbcInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {
    private final JdbcSourceSinkContext context;

    public JdbcInputFormat(JdbcSourceSinkContext context) {
        this.context = context;
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return null;
    }

    @Override
    public void configure(Configuration configuration) {
        //do nothing here
    }
}
