package Moudle.Spark1.sxt.suanfa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitonsWithIndexOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapPartitonsWithIndexOperator").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 准备一下数据
		List<String> names = Arrays
				.asList("xurunyun", "liangyongqi", "wangfei");
		
		JavaRDD<String> nameRDD = sc.parallelize(names,2);
		// 其实老师这个地方不写并行度2，默认其实它也是2 
		
		// parallelize并行集合的时候，指定了并行度为2，说白了就是numPartitions是2
		// 也就是说我们上面的三大女神会被分到不同的两个分区里面去！
		// 但是怎么分，我不知道，spark决定！！
		
		// 如果我想知道谁和谁分到了一组里面去？
		// MapPartitonsWithIndex这个算子可以拿到每个partition的index

		JavaRDD<String> nameWithPartitonIndex = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String name = iterator.next();
					String result = index + " : " + name;
					list.add(result);
				}
				return list.iterator();
			}
		}, true);
		
		nameWithPartitonIndex.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String result) throws Exception {
				System.out.println(result);
			}
		});
		
		sc.close();
	}
}
