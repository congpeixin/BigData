package Moudle.Spark1.sxt.sparkSQL

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/7/1.
  */
object JSONDataSource_scala {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JSONDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentScoresDF = sqlContext.read.json("students.json")

    //创建临时表，就是相当于在内存标记了以下，以后再去查询表，会直接去dataframe中去查找！
    studentScoresDF.registerTempTable("student_scores")
    val goodStudentNamesDF = sqlContext.sql("select name, score from student_scores where score >= 80")
    //DataFrame 可以直接使用RDD函数
    val goodStudentNames = goodStudentNamesDF.map(x => x(0)).collect()

    var studentInfoJSONs = List[String]()
    studentInfoJSONs = studentInfoJSONs.::("{\"name\":\"Yasaka\",\"age\":18}")
    studentInfoJSONs = studentInfoJSONs.::("{\"name\":\"Xuruyun\",\"age\":17}")
    studentInfoJSONs = studentInfoJSONs.::("{\"name\":\"Liangyongqi\",\"age\":19}")

    val studentInfosRDD = sc.parallelize(studentInfoJSONs)
    val studentInfosDF = sqlContext.read.json(studentInfosRDD)
    studentInfosDF.registerTempTable("student_infos")

    var sql = "select name, age from student_infos where name in ("
    var i=0
    for(name <- goodStudentNames){
      sql += "'" + name + "'"
      if (i < goodStudentNames.length - 1){
        sql += ","
      }
      i += 1
    }
    sql = sql + ")"

    val goodStudentInfosDF = sqlContext.sql(sql)

    val goodStudentsRDD = studentScoresDF.map(x => (x(0),x(1))).join(goodStudentInfosDF.map(x => (x(0),x(1))))
    val goodStudentsRDDRow = goodStudentsRDD.map(x=> Row(x._1.toString, x._2._1.toString.toInt, x._2._2.toString.toInt))

    var arr = Array(StructField("name",StringType,true)
                    ,StructField("score",IntegerType,true)
                    ,StructField("age",IntegerType,true))
    val structType = StructType(arr)

    val goodStudentDF = sqlContext.createDataFrame(goodStudentsRDDRow, structType)
    goodStudentDF.write.format("json").mode(SaveMode.Overwrite).save("goodStudentJson")

  }
}
