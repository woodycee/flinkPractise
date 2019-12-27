package com.woody.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 下程序从一系列整数中连续减去1，直到它们达到零为止
 */
public class IterativeStreamTest {
    public static void main(String[] args) throws Exception{
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().setParallelism(1);
        //生成0到 5的整数
        DataStream<Long> someIntegers = env.generateSequence(0, 5);
        //someIntegers.print();

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });
        minusOne.print("minusOne");

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        //stillGreaterThanZero.print();
        iteration.closeWith(stillGreaterThanZero);
        stillGreaterThanZero.print("stillGreaterThanZero");
//        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long value) throws Exception {
//                return (value <= 0);
//            }
//        });
        System.out.println("===========================================");
        //lessThanZero.print();

        env.execute("woody");
    }
}
