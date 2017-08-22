package Moudle.Spark1.sxt.suanfa;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class FilterOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// filter算子是过滤，里面的逻辑如果返回的是true就保留下来，false就过滤掉
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

		JavaRDD<Integer> results = numberRDD
				.filter(new Function<Integer, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Integer number) throws Exception {
						return number % 2 == 0;
					}
				});
		
		results.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer result) throws Exception {
				System.out.println(result);
			}
		});

		sc.close();
	}
}
