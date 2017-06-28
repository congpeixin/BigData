package Moudle.Spark1.sxt.cong.sparkSQL

import java.sql.DriverManager
import java.util

import org.apache.spark.sql.types.{StructType, IntegerType, StringType, StructField}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/7/5.
  */
object JDBCDataSource {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    var options = new util.HashMap[String,String]()
    options.put("url","jdbc:mysql://spark001:3306/test")
    options.put("user","root")
    options.put("password","123123")
    options.put("dbtable","student_infos")

    val studentInfosDF = sqlContext.read.format("jdbc").options(options).load()

    options.put("dbtable","student_scores")
    val studentScoresDF = sqlContext.read.format("jdbc").options(options).load()

    // 将两个DataFrame数据框转换成PairRDD，进行JOIN操作
    val rdd1 = studentInfosDF.map(x => (x.getString(0), x.getInt(1)))
    val rdd2 = studentScoresDF.map(x => (x.getString(0), x.getInt(1)))
    val studentsRDD = rdd1.join(rdd2)

    // 将PairRDD转换为RDD<Row>
    val studentsRowRDD = studentsRDD.map(x => Row(x._1, x._2._1, x._2._2))
    val goodStudentsRDD = studentsRowRDD.filter(x => x.getInt(2)>80)

    val structFields = Array(StructField("name", StringType, true)
                            ,StructField("age", IntegerType, true)
                            ,StructField("score", IntegerType, true))

    val studentsDF = sqlContext.createDataFrame(goodStudentsRDD, StructType(structFields))
    for(row <- studentsDF){
      println(row)
    }

    studentsDF.foreach( row => {
        val sql = "insert into good_student_infos values(" +"'" + row.getString(0) + "',"  +  row.getInt(1) + "," +  row.getInt(2) + ")"
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://spark001:3306/test","root","123123")
        val stat = conn.createStatement()
        stat.executeUpdate(sql)

        if(stat != null){
          stat.close()
        }
        if(conn != null){
          conn.close()
        }
      }
    )
  }
}
