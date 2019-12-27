package com.woody.flink;

import com.alibaba.fastjson.JSONObject;
import com.woody.flink.domain.KafkaMessage;
import com.woody.flink.function.TestCountFlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 开启checkpoint后不设置fsstatebackend或rocksdbbackend，
 * 则会把状态存在taskmanager节点内存中，checkpoint会放在jobmanager的内存中，
 * 实际上只有rocksdbbackend是把运行时的状态数据存在内存之外，其他两个均是把运行时的状态数据存在内存中。
 * ==========================================================================================
 * MemoryStateBackend: state数据保存在java堆内存中，执行checkpoint的时候，会把state的快照数据保存到jobmanager的内存中。
 * FsStateBackend: state数据保存在taskmanager的内存中，执行checkpoint的时候，会把state的快照数据保存到配置的文件系统中，可以使用hdfs等分布式文件系统。 FsStateBackend有一个策略，当状态的大小小于1MB（可配置，最大1MB）时，会把状态数据直接存储在meta data file中，避免出现很小的状态文件。
 * RocksDBStateBackend: RocksDB跟上面的都略有不同，它会在本地文件系统中维护状态，state会直接写入本地rocksdb中。同时RocksDB需要配置一个远端的filesystem。RocksDB克服了state受内存限制的缺点，同时又能够持久化到远端文件系统中，比较适合在生产中使用。
 */
public class ManagedKeyedStateStreamingTest {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("woody",8081,"D:\\practise\\flinkPractice\\stateTest\\target\\stateTest-1.0-SNAPSHOT.jar");
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);//时间特性
        env.setRestartStrategy(RestartStrategies.noRestart()); //重启策略
        //env.setStateBackend(new FsStateBackend("hdfs://woody:9000/flink/checkpoints"));//设置状态保存地方  也可以在全局设置 如为file:///,则在taskmanager的本地
        /*
         * 默认情况下，如果设置了Checkpoint选项，则Flink只保留最近成功生成的1个Checkpoint，而当Flink程序失败时，可以从最近的这个Checkpoint来进行恢复。但是，如果希望保留多个Checkpoint，并能够根据实际需要选择其中一个进行恢复，这样会更加灵活，比如，发现最近4个小时数据记录处理有问题，希望将整个状态还原到4小时之前。
         * Flink可以支持保留多个Checkpoint，需要在Flink的配置文件conf/flink-conf.yaml中，添加如下配置，指定最多需要保存Checkpoint的个数：
         */
        //检查点
        env.enableCheckpointing(5000);//检查点
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //确保一次性语义
        checkpointConfig.setMinPauseBetweenCheckpoints(500);// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        checkpointConfig.setCheckpointTimeout(60000);// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION： 取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION： 取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//退出不删除检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "woody:9092");//kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty("group.id", "test-consumer-group");//flink consumer flink的消费者的group.id

        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer<String>("woody", new SimpleStringSchema(), properties);
        env.addSource(myConsumer).map(new MapFunction<String, Tuple3<String,String,Long>>() {
            @Override
            public Tuple3<String,String,Long> map(String value) throws Exception {
                KafkaMessage parse = JSONObject.parseObject(value, KafkaMessage.class);
                String jsonString = parse.getJsonString();
                String[] split = jsonString.split("\\|");
                return new Tuple3<String,String,Long>(split[0],split[1],Long.valueOf(split[2]));
            }
        }).keyBy(0).flatMap(new TestCountFlatMapFunction()).print();

        env.execute("ManagedKeyedStateStreamingTest");
    }
}
