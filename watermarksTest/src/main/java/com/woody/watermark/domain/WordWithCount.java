package com.woody.watermark.domain;

import lombok.Data;

import java.io.Serializable;

@Data
public class WordWithCount implements Serializable {
    public String word;
    public long count;
    public long time;

    public WordWithCount() {}

    public WordWithCount(String word, long count,long time) {
        this.word = word;
        this.count = count;
        this.time = time;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
