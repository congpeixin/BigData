//package Moudle.Spark1.sxt.cong.sparkSQL
//
//import org.apache.spark.sql.types.{StructType, IntegerType, StringType, StructField}
//import org.apache.spark.sql.{SaveMode, Row, SQLContext}
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
//	* Created by root on 2016/8/31.
//	*/
//object JSONDataSource_Test {
//	def main(args: Array[String]) {
//		val conf = new SparkConf().setAppName("JSONDataSource").setMaster("local")
//		val sc = new SparkContext(conf)
//		val sqlContext = new SQLContext(sc)
//
//		val studentsoreDT = sqlContext.read.json("students.json")
//
//		studentsoreDT.registerTempTable("studentsore")
//
//		val goodstudentsoreDT = sqlContext.sql("select name , score from studentsore where score > 80")
//
//		val goodstudentName = goodstudentsoreDT.map(x => x(0)).collect()
//
//		var studentage = List[String]()
//
//		studentage = studentage.::("{\"name\":\"Cpeixin\", \"age\":22}")
//		studentage = studentage.::("{\"name\":\"Nazha\", \"age\":23}")
//		studentage = studentage.::("{\"name\":\"Hyuting\", \"age\":24}")
//		studentage = studentage.::("{\"name\":\"Zchuang\", \"age\":25}")
//
//		val studentageRDD = sc.parallelize(studentage)
//
//		val studentageDT = sqlContext.read.json(studentageRDD)
//
//		studentageDT.registerTempTable("studentage")
//
//		var sql = "select name, age from studentage where name in ("
//		var i=0
//		for(name <- goodstudentName){
//			sql += "'" + name + "'"
//			if (i < goodstudentName.length - 1){
//				sql += ","
//			}
//			i += 1
//		}
//		sql = sql + ")"
//
//		val goodstudentageDT = sqlContext.sql(sql)
//
//		val goodstudentInfo = goodstudentsoreDT.map(x => (x(0),x(1))).join(goodstudentageDT.map(x => (x(0),x(1))))
//		val studentInfo = goodstudentInfo.map(x => Row(x._1.toString, x._2._1.toString.toInt, x._2._2.toString.toInt))
//
//
//		var arr = Array(StructField("name",StringType,true)
//			,StructField("score",IntegerType,true)
//			,StructField("age",IntegerType,true))
//		val structType = StructType(arr)
//
//		val goodStudentDF = sqlContext.createDataFrame(studentInfo, structType)
//		//goodStudentDF.write.format("json").mode(SaveMode.Overwrite).save("goodStudentJson_2")
//
//
//
//
//
//
//
//		goodStudentDF.sort("score").foreach(a => println(a))
//
//
//
//
//	}
//}
