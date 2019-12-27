package com.woody.flink.source;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;
import java.util.List;

public class JavaCollectionSourceDemo {
    /**
     * 注意：当前，集合数据源要求数据类型和迭代器实现 Serializable。此外，收集数据源不能并行执行（并行度= 1）。
     */
//    public static void main(String[] args) {
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        //Create a DataStream from a list of elements
//        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(0, 1, 23, 2342);
//
//        //Create a DataStream from any Java collection
//        List<Tuple2<String, Integer>> data = Lists.newArrayList();
//        for (int i = 0; i < 5 ; i++) {
//            Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<String, Integer>();
//            stringIntegerTuple2.f0 = "woody";
//            stringIntegerTuple2.f1 = i;
//            data.add(stringIntegerTuple2);
//        }
//        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.fromCollection(data);
//
//        //Create a DataStream from an Iterator
//        List<Long> asda = Lists.newArrayList();
//        Iterator<Long> iterator = asda.iterator();
//        DataStreamSource<Long> tuple2DataStreamSource1 = env.fromCollection(iterator, Long.class);
//    }
}
