package com.woody.watermark;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;

/**
 * 如果当前数据的 EventTime 在 WaterMark 之上，也就是 EventTime> WaterMark。因为我们知道数据所属窗口的 WindowEndTime，一定是大于 EventTime 的。这时我们有 WindowEndTime > EventTime > WaterMark。所以这种情况下数据是一定不会丢失的。
 * 如果当前数据的 EventTime 在 WaterMark 之下，也就是 WaterMark > EventTime。这时候要分两种情况：
 *   2.1 如果该数据所属窗口的 WindowEndTime > WaterMark，则表示窗口还没被触发，即 WindowEndTime > WaterMark > EventTime，这种情况数据也是不会丢失的。
 *   2.1 如果该数据所属窗口的 WaterMark > WindowEndTime，则表示窗口已经无法被触发，即 WaterMark > WindowEndTime > EventTime，这种情况数据也就丢失了。
 * ————————————————
 * 版权声明：本文为CSDN博主「宇毅」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/xsdxs/article/details/82415450
 */
public class WatermarksTest {
    public static void main(String[] args) throws Exception {
        long delay = 5100L;
        int windowSize = 15;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置数据源
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, String, Long>> dataStream = env.addSource(new WatermarksTest.DataSource()).name("Demo Source");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> watermark = dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {
            private final long maxOutOfOrderness = delay;
            private long currentMaxTimestamp = 0L;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f2;
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                System.out.println(element.f1 + " -> " + timestamp + " -> " + format.format(timestamp));
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });

        // 窗口函数进行处理
        DataStream<Tuple3<String, String, Long>> resStream = watermark.keyBy(0).timeWindow(Time.seconds(windowSize)).reduce(
                new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                        return Tuple3.of(value1.f0, value1.f1 + "" + value2.f1, 1L);
                    }
                }
        );

        resStream.print();

        env.execute("event time demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws InterruptedException {
            Tuple3[] elements = new Tuple3[]{
                    Tuple3.of("a", "1", 1000000050000L),
                    Tuple3.of("a", "2", 1000000054000L),
                    Tuple3.of("a", "3", 1000000079900L),
                    Tuple3.of("a", "4", 1000000115000L),
                    Tuple3.of("b", "5", 1000000100000L),
                    Tuple3.of("b", "6", 1000000108000L)
            };

            int count = 0;
            while (running && count < elements.length) {
                ctx.collect(new Tuple3<>((String) elements[count].f0, (String) elements[count].f1, (Long) elements[count].f2));
                count++;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
