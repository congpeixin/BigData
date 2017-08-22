package Moudle.scala

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2017/6/25.
  *
  * 广播变量的使用: 利用广播变量过滤黑名单
  *
  */
object BroadcastTest {
  case class User(name: String)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouse = "/spark-warehouse"
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("DataSetBlackName")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()
    import spark.implicits._
    val userDS = spark.read.text("data/AllName.txt").as[String]
    val blackUserDS = spark.read.text("data/blackName.txt").as[String]

    val blackname = spark.sparkContext.broadcast(util.Arrays.asList("周永康","令计划","郭伯雄"))

    val whiteDS = userDS.filter(name => !(blackname.value.contains(name)))
    ////
    ////
    whiteDS.show()




  }
}
