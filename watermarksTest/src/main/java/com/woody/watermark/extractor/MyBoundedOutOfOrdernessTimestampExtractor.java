package com.woody.watermark.extractor;

import com.woody.watermark.domain.WordWithCount;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 周期性水印生成的另一个示例是水印在流中看到的最大（事件时间）时间戳落后固定时间量的情况。这种情况包括预先知道流中可能遇到的最大延迟的场景
 */
public class MyBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<WordWithCount> {


    public MyBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(WordWithCount element) {
        return 0;
    }
}
