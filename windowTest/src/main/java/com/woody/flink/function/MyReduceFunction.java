package com.woody.flink.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyReduceFunction implements ReduceFunction<Tuple2<String,Long>> {
    @Override
    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
        t1.setField(stringLongTuple2.f1 + t1.f1,1);
        return t1;
    }
}
