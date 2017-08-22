//package Moudle.Spark1.sxt.scala;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import scala.Tuple2;
//import java.util.Arrays;
//
///**
// * Created by root on 2016/8/27.
// */
//public class JavaWordCountTest {
//    public static void main(String[] args){
//        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local");
//        JavaSparkContext sc  = new JavaSparkContext(conf);
//
//        JavaRDD<String> lines = sc.textFile("word");
//
//        JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
//
//
//            private static final long serialVersionUID = 1L;
//            public Iterable<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(" "));
//            }
//        });
//
//        JavaPairRDD<String, Integer> pair = word.mapToPair(new PairFunction<String, String, Integer>() {
//
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<String, Integer>(word, 1);
//            }
//        });
//
//        JavaPairRDD result = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//
//        result.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                System.out.println("word: " + tuple._1 + " count:" + tuple._2);
//            }
//        });
//
//
//
//    }
//}
