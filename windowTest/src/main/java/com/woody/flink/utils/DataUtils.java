package com.woody.flink.utils;

public class DataUtils {

    private final String json = "";
    //根据传入的地区算出所在的省份
    public static String getProvinceFromArea(String area){
         if("上海".equals(area)){
             return "上海";
         }
         if("桂林".equals(area)){
             return "广西";
         }
         return "";
    }


}
