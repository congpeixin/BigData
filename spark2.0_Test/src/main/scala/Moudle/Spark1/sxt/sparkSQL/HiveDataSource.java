package Moudle.Spark1.sxt.sparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataSource {

	/*
	 * ./bin/spark-submit --master local --class com.spark.study.sql.HiveDataSource --driver-class-path ./lib/mysql-connector-java-5.1.32-bin.jar --files ./conf/hive-site.xml sparksql.jar
	 * 我们要用hive，是不是mysql得保证启动着吧
	 * 我们的HDFS得启动着吧 
	 */
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("dataframe");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 这里主要它要的是SparkContext
		HiveContext hiveContext = new HiveContext(sc.sc());
		
		// 判读是否存储student_infos表,如果存储则删除
		hiveContext.sql("DROP TABLE IF EXISTS student_infos");
		// 重建
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos ( name STRING, age INT)");
		// 加载数据,这里面是HIVE的东西,我们主要是讲spark sql,所以HIVE的东西我们就不多言了
		hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/hadoopsoft/spark-1.3.1-bin-hadoop2.4/student_infos.txt' "
				+ "INTO TABLE student_infos");
		
		// 一样的方式导入其它表
		hiveContext.sql("DROP TABLE IF EXISTS student_scores");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores ( name STRING, score INT)");
		hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/hadoopsoft/spark-1.3.1-bin-hadoop2.4/student_scores.txt' "
				+ "INTO TABLE student_scores");
		
		// 关联两张表,查询成绩大于80分的学生
		
		DataFrame goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
				+ "FROM student_infos si "
				+ "JOIN student_scores ss ON si.name=ss.name "
				+ "WHERE ss.score>=80");
		
		// 我们得到的这个数据是不是还得存回HIVE表中啊
		hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");  
		goodStudentsDF.saveAsTable("good_student_infos"); 
		
		// 然后如果是一个HIVE表我们怎么给它读入进来变成一个DataFrame呢
		DataFrame temp = hiveContext.table("good_student_infos");
		Row[] rows = temp.collect();
		for(Row row : rows){
			System.out.println(row);
		}
		
		sc.close();
	}
}
