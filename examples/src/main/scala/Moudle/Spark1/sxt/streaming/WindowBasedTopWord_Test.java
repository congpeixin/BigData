//package Moudle.Spark1.sxt.streaming;
//
//import breeze.optimize.linear.LinearProgram;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import scala.Tuple2;
//
///**
// * Created by root on 2016/9/5.
// */
//public class WindowBasedTopWord_Test {
//    public static void main(String[] args){
//        SparkConf conf = new SparkConf().setAppName("searchword").setMaster("local");
//        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//
//        JavaReceiverInputDStream<String> searcword = jsc.socketTextStream("node1",8888);
//
//        JavaDStream<String> searchwordDT = searcword.map(new Function<String,String>() {
//            @Override
//            public String call(String s) throws Exception {
//                return s.split(" ")[1];
//            }
//        });
//
//        JavaPairDStream<String, Integer> searchwordKVDT = searchwordDT.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s, 1);
//            }
//        });
//
//        JavaPairDStream<String, Integer> searchWordCountsDStream = searchwordKVDT.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer+integer2;
//            }
//        },Durations.seconds(60),Durations.seconds(10));
//
//        JavaPairDStream<String,Integer> finalDStream = searchWordCountsDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
//
//
//            @Override
//            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
//                return null;
//            }
//        });
//
//
//
//
//    }
//}
