package com.woody.watermark.extractor;

import com.woody.watermark.domain.WordWithCount;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;


/**
 * kafka的分区时间戳
 */
public class MyAscendingTimestampExtractor extends AscendingTimestampExtractor<WordWithCount> {
    Long currentMaxTimestamp = 0L;
    Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10s
    @Override
    public long extractAscendingTimestamp(WordWithCount element) {
        long timestamp = element.getTime();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        System.out.println("timestamp :"+ timestamp +","+element.getWord()+  " currentMaxTimestamp "+ currentMaxTimestamp);
        return timestamp;
    }

}
