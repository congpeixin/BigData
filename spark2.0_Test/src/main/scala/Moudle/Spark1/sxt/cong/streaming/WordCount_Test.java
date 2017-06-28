package Moudle.Spark1.sxt.cong.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by root on 2016/9/2.
 */
public class WordCount_Test {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("streaming").setMaster("local[2]");
        //设置JavaStreaming上下文，设置每收集多久时间的数据去分配一个batch去处理
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("node1",8888);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {


            public Iterable<String> call(String s) throws Exception {

                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairDStream<String, Integer> kvword = words.mapToPair(new PairFunction<String, String, Integer>() {


            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairDStream<String, Integer> wordcount = kvword.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        wordcount.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();



    }
}
