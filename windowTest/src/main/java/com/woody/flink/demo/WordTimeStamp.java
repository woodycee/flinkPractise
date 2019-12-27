package com.woody.flink.demo;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class WordTimeStamp extends AscendingTimestampExtractor<Tuple3<String, Long, Long>> {
    @Override
    public long extractAscendingTimestamp(Tuple3<String, Long, Long> element) {
        return element.f2;
    }
}
