package com.woody.watermark.way;

import com.woody.watermark.domain.WordWithCount;
import lombok.Data;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 为数据流中的每一个递增的Event Time 创建一个水印
 * **实际生产环境中，如果遇到高TPS 会对下游的算子造成较大的压力
 */
@Data
public class MyAssignerWithPeriodicWatermarks<W> implements AssignerWithPeriodicWatermarks<WordWithCount> {
    private Long currentMaxTimestamp = 0L;
    private Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10s
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(WordWithCount element, long previousElementTimestamp) {
        long timestamp = element.getTime();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        System.out.println("timestamp :"+ timestamp +","+element.getWord()+  " currentMaxTimestamp"+ currentMaxTimestamp);
        return timestamp;
    }
}
