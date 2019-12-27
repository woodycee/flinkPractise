/*
package com.woody.flink.flink.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class HbaseUtils {
    private static final String HBASE_POS = "192.168.21.221";
    private static Admin admin = null;
    private static Connection conn = null;

    */
/*** 静态构造，在调用静态方法前运行，  初始化连接对象  * *//*

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://" + HBASE_POS + ":9000/hbase");
        conf.set("hbase.zookeeper.quorum", HBASE_POS);
        conf.set("hbase.client.scanner.timeout.period", "6000000");
        conf.set("hbase.rpc.timeout", "6000000");
        //创建连接池
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //添加数据
    public static void addRow(String tableName, String rowKey, String columnFamily,
                             Map<String,String> columnvalue) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        //  插入数据，可通过 put(List<Put>) 批量插入
        byte[] familyBytes = columnFamily.getBytes();
        columnvalue.forEach((k,v)->{
            put.addColumn(familyBytes,k.getBytes(),v.getBytes());
        });
        table.put(put);
        table.close();
        conn.close();
    }


    //查询指定单cell内容
    public static String getCellData(String tableName, String rowKey, String family, String col){

        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(rowKey.getBytes());
            if(!get.isCheckExistenceOnly()){
                get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                result = Bytes.toString(resByte);
            }else{
                result = "查询结果不存在";
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "出现异常";
    }

}
*/
