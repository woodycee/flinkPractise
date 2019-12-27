///*
//package com.woody.flink.flink.function;
//
//import com.woody.flink.flink.domain.GclBase;
//import com.woody.flink.flink.util.HbaseUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.functions.MapFunction;
//
//import java.util.HashMap;
//
//
//public class GclBaseMap implements MapFunction<String, GclBase> {
//
//    private String tablename = "batteryInfo";
//    private String rowkey = "infoId";
//    private String familyname = "";
//
//
//    public GclBase map(String s) throws Exception {
//        if(StringUtils.isEmpty(s)){
//            return null;
//        }
//        String[] data = s.split(",");
//        //出入桩类型
//        String type = data[0];
//        //出入桩地区
//        String area = data[1];
//        //资产类型
//        String sku = data[2];
//
//        HashMap<String, String> params = new HashMap<>();
//        params.put("type",type);
//        params.put("area",area);
//        params.put("sku",sku);
//
//        HbaseUtils.addRow(tablename,rowkey,familyname,params);
//
//        return null;
//    }
//}
//*/
