package cn.arvidlw.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by lw_co on 2017/1/5.
 */
public class HBaseRecord {
    public static Put newPut(String rowkey,String cf,String qf,String val,long timeStamp){
        Put put = new Put(Bytes.toBytes(rowkey));//u作为rowkey
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(qf),timeStamp,Bytes.toBytes(val));
        return put;
    }
}
