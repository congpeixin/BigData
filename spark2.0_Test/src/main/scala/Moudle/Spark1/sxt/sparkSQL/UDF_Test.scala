package Moudle.Spark1.sxt.sparkSQL

import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
/**
	* Created by root on 2016/9/2.
	*/
object UDF_Test {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("UDF").setMaster("local[4]")
		val sc = new SparkContext(conf)
		val sqlcontext = new SQLContext(sc)
		val name = Array("Cpeixin", "Hyuting", "Nazha", "Zchuang")
		val nameRDD = sc.parallelize(name)
		//RDD转换成DF的一步，将RDD的格式转变成行格式
		val nameRowRDD = nameRDD.map(name => Row(name))
		//RDD转换成DF的一步，设置好转换格式
		val dftype = Array(StructField("name", StringType, true))
		val structType = StructType(dftype)
		//另一种简便的写法
		//val structType = StructType(Array(StructField("name",StringType,true)))
		//准备好了行格式的RDD,我们就可以使用createDataFrame进行创建DataFrame
		val nameDF = sqlcontext.createDataFrame(nameRowRDD, structType)
		//创建临时表，准备接下来进行UDF查询操作
		nameDF.registerTempTable("nameTable")
		//创建UDF函数
		sqlcontext.udf.register("nameLenght", (name: String) => name.length)
		sqlcontext.sql("select name,nameLenght(name) from nameTable").collect().foreach(name => println(name))

	}
}
