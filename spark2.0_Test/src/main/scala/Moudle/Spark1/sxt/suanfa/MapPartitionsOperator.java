//package Moudle.Spark1.sxt.suanfa;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//
//// 理解里面final使用的原因！
//
//public class MapPartitionsOperator {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("JoinOperator").setMaster("local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//
//		// 准备一下数据
//		List<String> names = Arrays.asList("xurunyun","liangyongqi","wangfei");
//		JavaRDD<String> nameRDD = sc.parallelize(names);
//
//		final Map<String, Integer> scoreMap = new HashMap<String, Integer>();
//		scoreMap.put("xurunyun", 150);
//		scoreMap.put("liangyongqi", 100);
//		scoreMap.put("wangfei", 90);
//
//		// mapPartitions
//		// map算子，一次就处理一个partition的一条数据！！！
//		// mapPartitions算子，一次处理一个partition中所有的数据！！！
//
//		// 推荐的使用场景！！！
//		// 如果你的RDD的数据不是特别多，那么采用MapPartitions算子代替map算子，可以加快处理速度
//		// 比如说100亿条数据，你一个partition里面就有10亿条数据，不建议使用mapPartitions，
//		// 内存溢出
//
//		JavaRDD<Integer> scoreRDD = nameRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<Integer> call(Iterator<String> iterator)
//					throws Exception {
//				List<Integer> list = new ArrayList<Integer>();
//
//				while(iterator.hasNext()){
//					String name = iterator.next();
//					Integer score = scoreMap.get(name);
//					list.add(score);
//				}
//
//				return list;
//			}
//		});
//
//		scoreRDD.foreach(new VoidFunction<Integer>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Integer score) throws Exception {
//				System.out.println(score);
//			}
//		});
//
//		sc.close();
//	}
//}
