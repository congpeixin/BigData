package Moudle.Spark1.sparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/6/27.
  */
object sparkSQL_json {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
      .builder().master("local[2]")  //本地运行
      .appName("sparkSQL_json")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.sqlContext.implicits._
    //两种读取方式都 ok
//    val cityDF = spark.sqlContext.read.format("json").load("data/person.json")
    val cityDF = spark.sqlContext.read.json("data/person.json")

    cityDF.show()
//    +---+-----+-----+---+
//    | id| name|  pwd|sex|
//    +---+-----+-----+---+
//    |  1| cong|xl123|  2|
//    |  2|   hu|xl123|  1|
//    |  3|zhang|xl123|  2|
//    +---+-----+-----+---+
    println("cityDF.show()")

    cityDF.printSchema()
//    root
//    |-- id: long (nullable = true)
//    |-- name: string (nullable = true)
//    |-- pwd: string (nullable = true)
//    |-- sex: long (nullable = true)
    println("cityDF.printSchema()")

    //接下来是根据列查询
    //select name,sex from table
    cityDF.select(cityDF.col("name"),cityDF.col("sex")).show()

    //select * from table where name = 'cong'
    cityDF.filter(cityDF.col("name")).show()

    cityDF.registerTempTable("table")
    cityDF.select("").show()

  }
}
