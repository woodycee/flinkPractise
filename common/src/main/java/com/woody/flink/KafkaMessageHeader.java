package com.woody.flink;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class KafkaMessageHeader implements Serializable,Cloneable{
    //消息惟一标识
    private String serialNo;

    //系统关键业务单号
    private String systemSourceId;

    //创建时间
    private Date createTime;

    //消息类型，用于区分不同业务消息
    private String messageType;


}
