package com.woody.flink.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ProcessWindowFunction获取一个Iterable，该Iterable包含窗口的所有元素，以及一个Context对象，该对象可以访问时间和状态信息，从而使其比其他窗口函数更具灵活性。这是以性能和资源消耗为代价的，因为无法增量聚合元素，而是需要在内部对其进行缓冲，直到将窗口视为已准备好进行处理为止。
 * 计算窗口中元素的数量。另外，窗口功能将有关窗口的信息添加到输出中。
 */
public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
        long count = 0;
        for (Tuple2<String, Long> in: input) {
            count++;
        }
        out.collect("Window: " + context.window() + "count: " + count);
    }
}
