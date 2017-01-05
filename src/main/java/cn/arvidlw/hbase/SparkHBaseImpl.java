package cn.arvidlw.hbase;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import akka.serialization.Serialization;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by lw_co on 2017/1/4.
 * 以SparkConf来读取HBase,返回Spark RDD
 */
public class SparkHBaseImpl {
    //final Logger logger = LoggerFactory.getLogger(AverageInfo.class);
    static Configuration conf= HBaseConfiguration.create();
    //static SparkConf confsp = new SparkConf().setAppName("AllInfo");
    static JavaSparkContext sc = new JavaSparkContext(
            new SparkConf()
                    .setAppName("keywords")
                    .setMaster("local[4]")//指定master,否则无法直接运行，要在vm中加参数
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
    );
//如果是这样的话就得运行时输入参数，可以在eidt configuration里配置，或者用spark-submit提交时给指定，否则会出错
//    static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("keywords"));
    public static void stop(){
        sc.stop();
    }

    public static JavaPairRDD<ImmutableBytesWritable, Result> getColumnValueFromHB( String tableName, String family, String qf, int version) throws IOException {

        Scan scan = new Scan();
        //scan.addFamily(Bytes.toBytes(family));
        scan.addColumn(Bytes.toBytes(family),Bytes.toBytes(qf));
        //can.setMaxResultSize(1);//限制每次client从HRegionServer取到的bytes总数,bytes总数通过row的KeyValue计算得出。所以不影响查询结果
        if (version <=0){version=1;}
        scan.setMaxVersions(version);
        //怎么感觉设没没都是scan那么多呢43238。因为row的个数，
        //scan.addColumn(Bytes.toBytes(f),Bytes.toBytes("temp"));
        //Filter filter=new SingleColumnValueFilter(Bytes.toBytes(f),Bytes.toBytes("Info"), CompareFilter.CompareOp.NOT_EQUAL,Bytes.toBytes("null"));
        //scan.setFilter();
        //Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum","10.3.9.135,10.3.9.231,10.3.9.232");
        //conf.set("hbase.zookeeper.property.clientPort","2222");
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("yarn-client");
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("spark://10.3.9.135:7077");
        //设置应用名称，就是在spark web端显示的应用名称，当然还可以设置其它的，在提交的时候可以指定，所以不用set上面两行吧
        //SparkConf confsp = new SparkConf().setAppName(appName);
        //.setMaster("local")//以本地的形式运行
        //.setJars(new String[]{"D:\\jiuzhouwork\\workspace\\hbase_handles\\out\\artifacts\\hbase_handles_jar\\hbase_handles.jar"});
        //创建spark操作环境对象
        //JavaSparkContext sc = new JavaSparkContext(confsp);
//        JavaSparkContext sc = new JavaSparkContext("yarn-client", "hbaseTest",
//                System.getenv("SPARK_HOME"), System.getenv("JARS"));
        //sc.addJar("D:\\jiuzhouwork\\other\\sparklibex\\spark-examples-1.6.1-hadoop2.7.1.jar");
        //从数据库中获取查询内容生成RDD
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return myRDD;
    }

    public static JavaPairRDD<ImmutableBytesWritable, Result> getColumnValueFromHB1(String tableName, String family, int version) throws IOException {

        Scan scan = new Scan();
//        scan.setCaching(100);//扫描缓存，进行一次RPC请求进行多少次扫描任务。即有批量任务时由同一请求发送
        scan.addFamily(Bytes.toBytes(family));
        //scan.setMaxResultSize(1);
        if (version <=0){version=1;}
        scan.setMaxVersions(version);//怎么感觉设没没都是scan那么多呢43238。因为row的个数，
        //scan.addColumn(Bytes.toBytes(f),Bytes.toBytes("temp"));
        //Filter filter=new SingleColumnValueFilter(Bytes.toBytes(f),Bytes.toBytes("Info"), CompareFilter.CompareOp.NOT_EQUAL,Bytes.toBytes("null"));
        Date date=new Date();
        //Calendar cal=Calendar.getInstance();
        SimpleDateFormat format=new SimpleDateFormat("yyyy_M_dd");
        String nowDate=format.format(date);
        //scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*"+nowDate+"$")));
        nowDate="2016_9_20";//正则匹配rowkey以这个日期结尾
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*"+nowDate+"$")));
        //scan.set

        //Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum","10.3.9.135,10.3.9.231,10.3.9.232");
        //conf.set("hbase.zookeeper.property.clientPort","2222");
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("yarn-client");
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("spark://10.3.9.135:7077");
        //设置应用名称，就是在spark web端显示的应用名称，当然还可以设置其它的，在提交的时候可以指定，所以不用set上面两行吧
        //SparkConf confsp = new SparkConf().setAppName(appName);
        //.setMaster("local")//以本地的形式运行
        //.setJars(new String[]{"D:\\jiuzhouwork\\workspace\\hbase_handles\\out\\artifacts\\hbase_handles_jar\\hbase_handles.jar"});
        //创建spark操作环境对象
        //JavaSparkContext sc = new JavaSparkContext(confsp);
//        JavaSparkContext sc = new JavaSparkContext("yarn-client", "hbaseTest",
//                System.getenv("SPARK_HOME"), System.getenv("JARS"));
        //sc.addJar("D:\\jiuzhouwork\\other\\sparklibex\\spark-examples-1.6.1-hadoop2.7.1.jar");
        //从数据库中获取查询内容生成RDD
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return myRDD;
    }

}
