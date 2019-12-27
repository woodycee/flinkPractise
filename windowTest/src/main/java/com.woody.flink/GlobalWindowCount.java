package com.woody.flink;

import com.woody.flink.demo.WordSource;
import com.woody.flink.demo.WordTimeStamp;
import com.woody.watermark.domain.WordWithCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class GlobalWindowCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(params);

        List<WordWithCount> input = Lists.newArrayList();
        for (int i = 0; i < 6; i++) {
            WordWithCount wordWithCount = new WordWithCount();
            wordWithCount.setWord("woody" + i);
            wordWithCount.setCount(i);
            input.add(wordWithCount);
        }
        WordSource wordSource = new WordSource(input);
        DataStreamSource<Tuple3<String, Long, Long>> tuple4DataStreamSource = env.addSource(wordSource);


        int evictionSec = 10;
        double triggerMeters = 50;
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> tuple4SingleOutputStreamOperator = tuple4DataStreamSource
                .assignTimestampsAndWatermarks(new WordTimeStamp())
                //Specifying keys via field positions is only valid for tuple data types
                .keyBy(0)
                .window(GlobalWindows.create())
                .evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                .trigger(DeltaTrigger.of(triggerMeters,
                        new DeltaFunction<Tuple3<String, Long, Long>>() {

                            @Override
                            public double getDelta(Tuple3<String, Long, Long> oldDataPoint, Tuple3<String, Long, Long> newDataPoint) {
                                long l = oldDataPoint.f1 - newDataPoint.f1;
                                return Double.parseDouble("" + l);
                            }
                        }, tuple4DataStreamSource.getType().createSerializer(env.getConfig())))
                .maxBy(1);
        tuple4SingleOutputStreamOperator.setParallelism(1).print();
        env.execute("testGlobalwindows");
    }
}
