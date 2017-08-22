package Moudle.Spark1.sxt.cong.suanfa;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SecondSort {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SecondSort").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("sort.txt");
		JavaPairRDD<SecondSortKey, String> pairs = lines
				.mapToPair(new PairFunction<String, SecondSortKey, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<SecondSortKey, String> call(String line)
							throws Exception {
						String[] lineSplited = line.split(" ");
						SecondSortKey key = new SecondSortKey(
								Integer.valueOf(lineSplited[0]), 
								Integer.valueOf(lineSplited[1]));
						return new Tuple2<SecondSortKey, String>(key,line);
					}
				});

		JavaPairRDD<SecondSortKey, String> sortedPairs = pairs.sortByKey(false);
		
		JavaRDD<String> results = sortedPairs.map(new Function<Tuple2<SecondSortKey,String>,String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<SecondSortKey, String> tuple)
					throws Exception {
				return tuple._2;
			}
		});
		
		results.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String line) throws Exception {
				System.out.println(line);
			}
		});
		
		sc.close();
	}
}
