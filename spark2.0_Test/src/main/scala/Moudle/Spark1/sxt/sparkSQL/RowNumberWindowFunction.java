package Moudle.Spark1.sxt.sparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/*
 * 开窗函数
 */

public class RowNumberWindowFunction {

	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(sc.sc());

		// 创建销售额表，sales表
		hiveContext.sql("DROP TABLE IF EXISTS sales");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("
				+ "product STRING," + "category STRING," + "revenue BIGINT)");
		hiveContext.sql("LOAD DATA "
				+ "LOCAL INPATH '/usr/hadoopsoft/spark-1.5.0-bin-hadoop2.4/sales.txt' "
				+ "INTO TABLE sales");
		
		// 先说明一下，row_number()开窗函数的作用
		// 其实，就是给每个分组的数据，按照其排序顺序，打上一个分组内的行号
		// 比如说，有一个分组date=20151001，里面有3条数据，1122，1121，1124,
		// 那么对这个分组的每一行使用row_number()开窗函数以后，三行，依次会获得一个组内的行号
		// 行号从1开始递增，比如1121 1，1122 2，1124 3
		
		DataFrame top3SalesDF = hiveContext.sql(
				"SELECT product, category, revenue "
				+ "FROM (" 
				+  "SELECT "
					+ "product,"
					+ "category,"
					+ "revenue,"
					+ "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
					+ "FROM sales "  
				+ ") tmp_sales "
				+ "WHERE rank <= 2");
		
		// 将每组排名前3的数据，保存到一个表中
		hiveContext.sql("DROP TABLE IF EXISTS top3_sales");  
		top3SalesDF.saveAsTable("top3_sales");  
		
		sc.close();
	}
}
