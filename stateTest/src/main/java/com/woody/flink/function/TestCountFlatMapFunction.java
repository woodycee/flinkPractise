package com.woody.flink.function;

import lombok.val;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class TestCountFlatMapFunction extends RichFlatMapFunction<Tuple3<String,String,Long>,Tuple3<String,String,Long>> {

    private ValueState<Tuple3<String, String, Long>> valueState;
    @Override
    public void flatMap(Tuple3<String, String, Long> value, Collector<Tuple3<String, String, Long>> out) throws Exception {

        //显式调用已经过期的状态值会被删除，可以配置在读取快照时清除过期状态值，如:
//        StateTtlConfig build = StateTtlConfig
//                .newBuilder(Time.seconds(1))
//                .cleanupFullSnapshot().build();

        Tuple3<String, String, Long> temp = valueState.value();
        if(temp == null){
            temp = new  Tuple3<String, String, Long>(value.f0,value.f1,0L);
        }
        Tuple3 newSum = new Tuple3<String, String, Long> (temp.f0,temp.f1,value.f2 + temp.f2);
        valueState.update(newSum);
        out.collect(newSum);
    }

    @Override
    public void open(Configuration parameters) throws Exception{
        //设置状态值的过期时间
//        StateTtlConfig ttlConfig = StateTtlConfig
//                .newBuilder(Time.seconds(1))//过期时间1秒
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//在创建和写入时更新状态值
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//过期访问不返回状态值
//                .build();
        ValueStateDescriptor<Tuple3<String, String, Long>> valueStateDescriptor = new ValueStateDescriptor<>("sum", TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}));
//    valueStateDescriptor.enableTimeToLive(ttlConfig)//启用状态值过期配置
        valueState = getRuntimeContext().getState(valueStateDescriptor);

    }
}
