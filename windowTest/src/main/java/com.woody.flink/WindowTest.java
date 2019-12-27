package com.woody.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * 1 woody
 *
 */
@Slf4j
public class WindowTest {

    public static void main(String[] args) throws Exception{
        String hostname ;
        int port = 8900;
        try {
            ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("woody-s1") ? params.get("woody-s1") : "localhost";
        } catch (Exception var6) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --hostname <hostname> --port <port>', where hostname (localhost by default) and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and type the input text into the command line");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream(hostname, port, "\n");
        //基于时间窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(30))
                .sum(0)
                .print();*/

        //基于滑动时间窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(60), Time.seconds(30))
                .sum(0)
                .print();*/


        //基于事件数量窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(3)
                .sum(0)
                .print();*/

        //基于事件数量滑动窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(4, 3)
                .sum(0)
                .print();


        //基于会话时间窗口
       /* data.flatMap(new LineSplitter())
                .keyBy(1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(1))) //表示如果 1s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
                .sum(0)
                .print();*/

        env.execute("111");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<Long, String>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
            String[] tokens = s.split("\\s");

            if (tokens.length >= 2 && isValidLong(tokens[0])) {
                collector.collect(new Tuple2<>(Long.valueOf(tokens[0]), tokens[1]));
            }
        }
    }

    private static boolean isValidLong(String str) {
        try {
            long _v = Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            log.info("the str = {} is not a number", str);
            return false;
        }
    }
}
