package cn.arvidlw.app;

import akka.actor.ExtendedActorSystem;
import akka.serialization.Serialization;
import cn.arvidlw.hbase.SparkHBaseImpl;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.hankcs.hanlp.summary.TextRankKeyword;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lw_co on 2017/1/4.
 */
public class ReadTest {
    public static void main(String[] args) throws IOException {
        JavaPairRDD<ImmutableBytesWritable, Result> rdd = SparkHBaseImpl.getColumnValueFromHB("NEWSINFO", "info","content", 1);
        JavaPairRDD<String,String> rowContent=rdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> tu) throws Exception {
                byte[] v,k;
                v= tu._2().getValue(Bytes.toBytes("info"),Bytes.toBytes("content"));
                List<String> keywords= TextRankKeyword.getKeywordList(Bytes.toString(v),10);

                k=tu._2().getRow();//Method for retrieving the row key that corresponds to the row from which this Result was created.
                return new Tuple2<String, String>(Bytes.toString(k),Bytes.toString(v));
            }
        });
        List<Tuple2<String,String>> list=rowContent.collect();
        for (Tuple2<String,String> s : list) {
            System.out.println(s._1+":"+s._2);
        }
//        List<Tuple2<ImmutableBytesWritable, Result>> ss=rdd.collect();
//        for (Tuple2<ImmutableBytesWritable, Result> t: ss ) {
//            byte[] v=t._2().getValue(Bytes.toBytes("info"),Bytes.toBytes("content"));
//            System.out.println(Bytes.toString(v));//Bytes.toString(v)之后才会把字节码所对应的字符串给输出来。
//        }
//        System.out.println(ss.size());
    }
}
