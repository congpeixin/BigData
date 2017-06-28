package Moudle.Spark1.sxt.cong.suanfa;//package com.cong.suanfa;
//
//import java.util.Arrays;
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.VoidFunction;
//
//import scala.Tuple2;
//
//// reduceByKey = groupByKey + reduce
//// shuffle 洗牌  = map端 + reduce端
//// spark里面这个reduceByKey在map端自带Combiner
//
//public class ReduceByKeyOperator {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster(
//				"local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//
//		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
//				new Tuple2<String, Integer>("xuruyun" , 150),
//				new Tuple2<String, Integer>("liangyongqi" , 100),
//				new Tuple2<String, Integer>("wangfei" , 100),
//				new Tuple2<String, Integer>("wangfei" , 80));
//
//		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scoreList);
//
//		rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Integer call(Integer v1, Integer v2) throws Exception {
//				return v1+v2;
//			}
//		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<String, Integer> tuple) throws Exception {
//				System.out.println("name ： " + tuple._1 + " score :" + tuple._2);
//			}
//		});
//
//		sc.close();
//	}
//}
