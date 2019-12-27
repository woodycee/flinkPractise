package com.woody.watermark.domain;

import lombok.Data;
import org.apache.flink.api.java.functions.KeySelector;

@Data
public class MyEvent implements KeySelector {

    private String info;
    private Long creationTime;

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public Long getCreationTime() {
        if(creationTime == null){
            creationTime = System.currentTimeMillis();
        }
        return creationTime;
    }

    public void setCreationTime(Long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public Object getKey(Object o) throws Exception {
        return info;
    }

    public boolean hasWatermarkMarker() {
        return creationTime == null;
    }
}
