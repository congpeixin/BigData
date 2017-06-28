package Moudle.Spark1.sxt.sparkSQL;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class ParquetDataSource {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame usersDF = sqlContext.read().parquet("users.parquet");
		usersDF.registerTempTable("users");
		
		DataFrame userNamesDF = sqlContext.sql("select * from users");
		List<String> names = userNamesDF.javaRDD().map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return "Name : " + row.getString(0);
			}
		}).collect();
		
		for(String name : names){
			System.out.println(name);
		}
	}
}
