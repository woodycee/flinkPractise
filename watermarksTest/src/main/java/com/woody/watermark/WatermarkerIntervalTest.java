package com.woody.watermark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WatermarkerIntervalTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置自动水印发射的间隔。您可以使用获取当前值long
        env.getConfig().setAutoWatermarkInterval(1000);
        long autoWatermarkInterval = env.getConfig().getAutoWatermarkInterval();
        System.out.println(autoWatermarkInterval);
    }
}
