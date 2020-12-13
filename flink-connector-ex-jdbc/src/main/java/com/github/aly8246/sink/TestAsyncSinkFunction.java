package com.github.aly8246.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.Collections;

public class TestAsyncSinkFunction extends RichAsyncFunction<Tuple2<Boolean, Row>, Object> {
    /**
     * 初始化
     */
    @Override
    public void open(Configuration parameters) throws Exception {
    }


    /**
     * 结束
     */
    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 异步消费接口
     */
    @Override
    public void asyncInvoke(Tuple2<Boolean, Row> input, ResultFuture<Object> resultFuture) {
        System.out.println("input = " + input + ", resultFuture = " + resultFuture);
        resultFuture.complete(Collections.singleton(input.f1.toString()));
    }
}
