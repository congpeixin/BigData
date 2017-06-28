package Moudle.Spark1.sxt.suanfa;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class AccumulatorValue {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("AccumulatorValue").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		final Accumulator<Integer> sum = sc.accumulator(0);
		List<Integer> list = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> listRDD = sc.parallelize(list,4);
		
		listRDD.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer value) throws Exception {
				sum.add(value);
//				System.out.println(sum.value());
			}
		});
		
		System.out.println(sum.value());
		
		sc.close();
	}
}
