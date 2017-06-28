package Moudle.Spark1.sxt.suanfa;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopN {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TopN").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("top.txt");
		
		JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(String line) throws Exception {
				return new Tuple2<Integer,String>(Integer.valueOf(line),line);
			}
		});
		
		JavaPairRDD<Integer, String> sorted = pairs.sortByKey(false);
		JavaRDD<String> results = sorted.map(new Function<Tuple2<Integer,String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, String> tuple) throws Exception {
				return tuple._2;
			}
		});
		
		List<String> list = results.take(3);
		for(String s : list){
			System.out.println(s);
		}
		
		sc.close();
	}
}
