package com.woody.flink;


import com.woody.watermark.way.MyAssignerWithPeriodicWatermarks;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * https://blog.csdn.net/xsdxs/article/details/82415450
 * 翻滚窗口
 */
public class TumblingWindowCount {
    public static void main(String[] args) throws Exception{
        String hostname = "localhost";
        int port = 8900;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ArrayList<String> strings = new ArrayList<>();
        strings.add("woody 1576581020000");
        strings.add("woody 1576581220000");
        strings.add("woody 1576581420000");
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(strings);
        //DataStreamSource<String> stringDataStreamSource = env.socketTextStream(hostname, port, "\n");
        SingleOutputStreamOperator<WordWithCount> wordWithCountSingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> out) throws Exception {
                String[] split = s.split("\\W+");
                try {
                    out.collect(new WordWithCount(split[0], 1L, Long.parseLong(split[1])));
                } catch (NumberFormatException e) {
                    System.out.println("===");
                }

            }
        });
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = wordWithCountSingleOutputStreamOperator.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks());

        //1 tumbling event-time windows
        DataStreamSink<WordWithCount> print = wordWithCountSingleOutputStreamOperator.keyBy("time")
                .window(TumblingEventTimeWindows.of(Time.seconds(1))).sum("count").name("socketWordCount").print();

        //2 tumbling processing-time windows
//        WindowedStream<String, Tuple, TimeWindow> words1 = input.keyBy("word")
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));


        //3 daily tumbling event-time windows offset by -8 hours.
        //可选offset 参数，该参数可用于更改窗口的对齐方式 窗口调整到比UTC-0时区等。例如，在中国，您必须指定的偏移量Time.hours(-8)。
//        WindowedStream<String, Tuple, TimeWindow> words2 = input.keyBy("word")
//                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)));

//        word.sum("count").name("TumblingEventTimeWindows ").print();

        env.execute("TumblingEventTimeWindows");



    }

    // Data type for words with count
    @Data
    public static class WordWithCount {

        public String word;
        public long count;
        public long time;

        public WordWithCount() {}

        public WordWithCount(String word, long count,long time) {
            this.word = word;
            this.count = count;
            this.time = time;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }


}
