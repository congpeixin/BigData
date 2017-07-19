package Moudle.Spark2.program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by root on 2017/7/18.
  * 分析用户行为
  * 读取parquet文件
  *
  */
object UserBehaviorsAnalysis {
//  Logger.getLogger("debug").setLevel(Level.ERROR)
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession.builder()
      .appName("userBehaviors")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._

    val data = spark.read.format("parquet").parquet("data/users.parquet")
//    data.show()
//    +------+--------------+----------------+
//    |  name|favorite_color|favorite_numbers|
//    +------+--------------+----------------+
//    |Alyssa|          null|  [3, 9, 15, 20]|
//    |   Ben|           red|              []|
//    +------+--------------+----------------+
    //读取parquet文件后生成DF
    //spark2.0中filter的写法
    data.filter($"name".equalTo("Ben")).select("name","favorite_color").show()
    //spark1.6中filter的写法
    data.filter(data("name").equalTo("Ben")).select("name","favorite_color").show()
    //另一种写法
    data.filter("name =="+"'Ben'").show()


  }
}
