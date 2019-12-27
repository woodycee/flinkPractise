package com.woody.watermark.way;

import com.woody.watermark.domain.MyEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 每隔一段时间或者数量，创建一个水印
 */
public class MyAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<MyEvent> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(MyEvent lastElement, long extractedTimestamp) {
        return lastElement.hasWatermarkMarker() ? new Watermark(extractedTimestamp) : null;
    }

    @Override
    public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
        return element.getCreationTime();
    }
}
