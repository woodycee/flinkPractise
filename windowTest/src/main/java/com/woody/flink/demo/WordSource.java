package com.woody.flink.demo;

import com.woody.watermark.domain.WordWithCount;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

@Data
public class WordSource implements SourceFunction<Tuple3<String,Long,Long>> {
    private List<WordWithCount> words;

    private volatile boolean isRunning = true;

    public WordSource(List<WordWithCount> words){
        this.words = words;
    }
    @Override
    public void run(SourceContext<Tuple3<String,Long,Long>> ctx) throws Exception {
        //写个死循环
        while(isRunning){
            Thread.sleep(100);
            if(CollectionUtils.isNotEmpty(words)){
                words.stream().forEach(wordWithCount -> {
                    long l = System.currentTimeMillis();
                    wordWithCount.setTime(l);
                    Tuple3<String, Long, Long> doubleTuple4 = new Tuple3<>();
                    doubleTuple4.f0 = wordWithCount.getWord();
                    doubleTuple4.f1 = wordWithCount.getCount();
                    doubleTuple4.f2 = wordWithCount.getTime();
                    ctx.collectWithTimestamp(doubleTuple4,l);
                });
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
