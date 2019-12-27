package com.woody.flink.function;

import org.apache.flink.api.common.functions.MapFunction;

public class EventTimeFunction implements MapFunction<String, Long> {
    @Override
    public Long map(String s) throws Exception {
        return null;
    }
}
