package com.woody.flink.source.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * Kafka消息,包含消息头和消息体
 */

@Data
public class KafkaMessage implements Serializable,Cloneable {

    /**
     * 统一消息头
     */
    protected KafkaMessageHeader messageHeader;

    /**
     * JSON格式消息体
     */
    private String jsonString;

    public KafkaMessage(String jsonString){
        this.jsonString = jsonString;
    }

    public KafkaMessage(KafkaMessageHeader messageHeader, String jsonString){
        this.messageHeader = messageHeader;
        this.jsonString = jsonString;
    }

}
