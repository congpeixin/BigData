package Moudle.Spark1.sxt.suanfa;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class JoinOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"),
				new Tuple2<Integer,String>(3, "annie"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100),
				new Tuple2<Integer,Integer>(3, 80));
		
		JavaPairRDD<Integer,String> nameRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer,Integer> scoreRDD = sc.parallelizePairs(scoreList);
		
		// JOIN关联两个RDD
		// JOIN操作会把相同的KEY的值放到一个集合里面去
		JavaPairRDD<Integer, Tuple2<String, Integer>> results = nameRDD.join(scoreRDD);
		
		results.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple)
					throws Exception {
				System.out.println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2);
			} 
		});
		
		sc.close();
	}
}
