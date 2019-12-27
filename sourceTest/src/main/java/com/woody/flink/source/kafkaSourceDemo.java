package com.woody.flink.source;

import com.alibaba.fastjson.JSONObject;
import com.woody.flink.source.common.WordWithCount;
import com.woody.flink.source.domain.KafkaMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 在0.9之前，Kafka没有提供任何机制来保证至少一次或精确一次语义。
 */
public class kafkaSourceDemo {
    public static void main(String[] args) throws Exception{
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("woody",8081,"D:\\practise\\flinkPractice\\sourceTest\\target\\sourceTest-1.0-SNAPSHOT.jar");
        env.enableCheckpointing(10000);// checkpoint every 5000 msecs
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "woody:9092");//kafka的节点的IP或者hostName，多个使用逗号分隔
       //properties.setProperty("zookeeper.connect", "woody:2181");//zookeeper的节点的IP或者hostName，多个使用逗号进行分隔  仅对于Kafka 0.8才需要
        properties.setProperty("group.id", "test-consumer-group");//flink consumer flink的消费者的group.id


        //FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer<>(Pattern.compile("test-topic-[0-9]"),new SimpleStringSchema(),properties);
        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer<String>("woody",new SimpleStringSchema(),properties);

        SingleOutputStreamOperator singleOutputStreamOperator = env.addSource(myConsumer).flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String,Long>> out) throws Exception {
                if (StringUtils.isNotEmpty(value)) {
                    KafkaMessage kafkaMessage = JSONObject.parseObject(value, KafkaMessage.class);
                    WordWithCount o = JSONObject.parseObject(kafkaMessage.getJsonString(), WordWithCount.class);
                    Tuple2<String, Long> stringLongTuple2 = new Tuple2<>();
                    stringLongTuple2.f0 = o.getWord();
                    stringLongTuple2.f1 = o.getCount();
                    out.collect(stringLongTuple2);
                }
            }
        });

        singleOutputStreamOperator.print("singleOutputStreamOperator");

        singleOutputStreamOperator.keyBy(0)
                .countWindow(2)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        value1.f1 = value2.f1 + value1.f1;
                        return value1;
                    }
                }).print("countWindow");

        singleOutputStreamOperator.keyBy(0)
                .timeWindow(Time.seconds(10))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        value1.f1 = value2.f1 + value1.f1;
                        return value1;
                    }
                }).print("timeWindow");



        env.execute("kafka source");
       // myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
    }
}
