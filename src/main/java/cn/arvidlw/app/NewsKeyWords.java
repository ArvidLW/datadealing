package cn.arvidlw.app;

import cn.arvidlw.hbase.HBaseClientImpl;
import cn.arvidlw.hbase.HBaseRecord;
import cn.arvidlw.hbase.SparkHBaseImpl;
import com.hankcs.hanlp.summary.TextRankKeyword;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by lw_co on 2017/1/4.
 */
public class NewsKeyWords {
    private static HBaseClientImpl hclient;
    private static String tableName="NEWSINFO";
    private static String cf="k";
    private static String qf="keys";//keywords
    private static final int PUTSMAXNUM=1000;
    public static void getkeyWords() throws IOException {
        JavaPairRDD<ImmutableBytesWritable, Result> rdd = SparkHBaseImpl.getColumnValueFromHB("NEWSINFO", "info","content", 1);
        JavaPairRDD<String,String> rowContent=rdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> tu) throws Exception {
                byte[] v,k;
                v= tu._2().getValue(Bytes.toBytes("info"),Bytes.toBytes("content"));
                List<String> keywords= TextRankKeyword.getKeywordList(Bytes.toString(v),10);
                StringBuilder keys=new StringBuilder();
                boolean flag=false;
                for (String s: keywords) {
                    if(flag){
                        keys.append(",");
                    }else {
                        flag=true;
                    }
                    keys.append(s);

                }
                k=tu._2().getRow();//Method for retrieving the row key that corresponds to the row from which this Result was created.
                return new Tuple2<String, String>(Bytes.toString(k),keys.toString());
            }
        });
        List<Tuple2<String,String>> list=rowContent.collect();
        List<Put> puts=new ArrayList<Put>();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        String vTimeStamp=sdf.format((new Date())).toString();
        for (Tuple2<String,String> s : list) {
            Put put= HBaseRecord.newPut(s._1,cf,qf,s._2,Long.valueOf(vTimeStamp));
            puts.add(put);
            if(puts.size()>PUTSMAXNUM){
                hclient.addRecordWithTsPuts(puts);
                puts.clear();
            }
            System.out.println(s._1+":"+s._2);
        }
        hclient.addRecordWithTsPuts(puts);

    }
    public static void main(String[] args) throws IOException {
        hclient=new HBaseClientImpl(tableName);
        //getkeyWords();
    }
}
