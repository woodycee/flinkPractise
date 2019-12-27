package com.woody.flink.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowCount {
    public static void main(String[] args) {
            String hostname;
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
            //配置检查点
            //setCheckPoint(env);
            DataStreamSource<String> text = env.socketTextStream(hostname, port, "\n");
             // The main required change is to manually specify operator IDs via the uid(String) method. These IDs are used to scope the state of each operator.
             //If you don’t specify the IDs manually they will be generated automatically.
            // You can automatically restore from the savepoint as long as these IDs do not change.
        // The generated IDs depend on the structure of your program and are sensitive to program changes.
        // Therefore, it is highly recommended to assign these IDs manually.

            DataStream<WordWithCount> windowCounts = text
                    .flatMap(new FlatMapFunction<String, WordWithCount>() {
                        @Override
                        public void flatMap(String value, Collector<WordWithCount> out) {
                            for (String word : value.split("\\s")) {
                                out.collect(new WordWithCount(word, 1L));

                            }
                        }
                    })
                    .keyBy("word")
                    .timeWindow(Time.seconds(5), Time.seconds(1))
                    .reduce(new ReduceFunction<WordWithCount>() {
                        @Override
                        public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                            return new WordWithCount(a.word, a.count + b.count);
                        }
                    });

            // print the results with a single thread, rather than in parallel
            windowCounts.print().setParallelism(1);

            try {
                env.execute("Socket Window WordCount-woody");
            } catch (Exception e) {
                e.printStackTrace();
            }

    }

    private static void setCheckPoint(StreamExecutionEnvironment env){
        // start a checkpoint every 10000 ms
        env.enableCheckpointing(10000);
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // make sure 500 ms of progress happen between checkpoints
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        //env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}

