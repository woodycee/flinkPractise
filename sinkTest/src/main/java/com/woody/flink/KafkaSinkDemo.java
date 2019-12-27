package com.woody.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;

public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        String kafkaTopic = "woody";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("woody",8081,"D:\\practise\\flinkPractice\\sinkTest\\target\\sinkTest-1.0-SNAPSHOT.jar");

        ArrayList<String> strings = new ArrayList<>();
        strings.add("woody 1576581020000");
        strings.add("woody 1576581220000");
        strings.add("woody 1576581420000");
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(strings);
        FlinkKafkaProducer myProducer = new FlinkKafkaProducer(
                "woody:9092",            // broker list
                kafkaTopic,                  // target topic
                new SimpleStringSchema());   // serialization schema
       // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
        // this method is not available for earlier Kafka versions
        myProducer.setWriteTimestampToKafka(true);

        DataStreamSink<String> stringDataStreamSink = stringDataStreamSource.addSink(myProducer);
//        env.addSource(new EventsGeneratorSource(errorRate, sleep))
//                .addSink(new FlinkKafkaProducer011<>(brokers, kafkaTopic, new EventDeSerializer()));


        // trigger program execution
        env.execute("State machine example Kafka events generator job");
    }
}
