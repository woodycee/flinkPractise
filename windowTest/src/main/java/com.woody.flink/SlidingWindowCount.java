package com.woody.flink;

import com.woody.watermark.domain.WordWithCount;
import com.woody.watermark.way.MyAssignerWithPeriodicWatermarks;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 大小为10分钟的窗口滑动5分钟。这样，您每隔5分钟就会得到一个窗口，其中包含最近10分钟内到达的事件，
 */
public class SlidingWindowCount {
    public static void main(String[] args) throws Exception {
        String hostname = "localhost";
        int port = 8900;

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
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
                out.collect(new WordWithCount(split[0], 1L, Long.parseLong(split[1])));

            }
        });

        wordWithCountSingleOutputStreamOperator.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks<WordWithCount>());

        //================================================================
// sliding event-time windows
        SingleOutputStreamOperator<WordWithCount> reduce = wordWithCountSingleOutputStreamOperator
                .keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount value) throws Exception {
                        return value.getWord();
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount wordWithCount, WordWithCount t1) throws Exception {

                        return new WordWithCount(wordWithCount.getWord(),wordWithCount.getCount() + t1.getCount(),t1.getTime());
                    }
                });

        reduce.print();

        env.execute("event time demo");

//// sliding processing-time windows
//        input
//                .keyBy(<key selector>)
//    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .<windowed transformation>(<window function>);
//
//// sliding processing-time windows offset by -8 hours
//        input
//                .keyBy(<key selector>)
//    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
//                .<windowed transformation>(<window function>);



    }

    /**
     * 源码中计算的窗口的起始点
     * @param timestamp
     * @param offset
     * @param windowSize
     * @return
     */
    private static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
    }
}
