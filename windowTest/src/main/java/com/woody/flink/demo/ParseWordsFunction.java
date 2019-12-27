package com.woody.flink.demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ParseWordsFunction  extends RichMapFunction<String, Tuple3<String,Long,Long>> {

    @Override
    public Tuple3<String, Long, Long> map(String value) throws Exception {
        String[] data = value.split(",");
        return new Tuple3<>(data[0], Long.valueOf(data[1]), Long.valueOf(data[2]));
    }
}
