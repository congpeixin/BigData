package Moudle.Spark1.sxt.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext;

object RowNumberWindowFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RowNumberWindowFunction").setMaster("local");
		val sc = new SparkContext(conf);
		val hiveContext = new HiveContext(sc);
		
		hiveContext.sql("DROP TABLE IF EXISTS sales")
		hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("
				+ "product STRING," + "category STRING," + "revenue BIGINT)")
		hiveContext.sql("LOAD DATA "
				+ "LOCAL INPATH '/home/spark-1.6.0-bin-hadoop2.4/sales.txt' "
				+ "INTO TABLE sales")
		
		val top3ScalesDF = hiveContext.sql("SELECT product, category, revenue "
				+ "FROM (" 
				+  "SELECT "
					+ "product,"
					+ "category,"
					+ "revenue,"
					+ "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
					+ "FROM sales "  
				+ ") tmp_sales "
				+ "WHERE rank <= 3")
		top3ScalesDF.saveAsTable("top3_sales")
				
  }
}