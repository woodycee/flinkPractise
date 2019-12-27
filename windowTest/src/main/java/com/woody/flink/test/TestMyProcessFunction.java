package com.woody.flink.test;

import com.woody.flink.function.MyProcessWindowFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;

public class TestMyProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStreamSource stringDataStreamSource = env.readFile(new TextInputFormat(Path.fromLocalFile(new File("D:\\practise\\flinkPractice\\windowTest\\src\\main\\resources\\TestKeyValue"))), "D:\\practise\\flinkPractice\\windowTest\\src\\main\\resources\\TestKeyValue");
        //DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\practise\\flinkPractice\\windowTest\\src\\main\\resources\\TestKeyValue");
        DataStreamSource<String> stringDataStreamSource = env.setParallelism(1).socketTextStream("localhost", 8900, "\n");
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = stringDataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                if(StringUtils.isNotEmpty(value)){

                    String[] split = value.split(",");
                    return new Tuple2<String, Long>(split[0], Long.parseLong(split[1]));
                }
                return null;
            }
        });
        SingleOutputStreamOperator<String> process = tuple2SingleOutputStreamOperator.keyBy(t -> t.f0)
                .timeWindow(Time.seconds(5))
                .process(new MyProcessWindowFunction());

        process.print().setParallelism(1);
        env.execute("TestMyProcessFunction");
    }
}
