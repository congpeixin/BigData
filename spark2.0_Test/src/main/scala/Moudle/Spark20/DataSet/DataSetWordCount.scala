package Moudle.Spark20.DataSet

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by cluster on 2017/5/25.
  */
object DataSetWordCount {
  val log = Logger.getLogger(DataSetWordCount.getClass)
  log.setLevel(Level.ERROR)
  val warehouse = "/spark-warehouse"
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("DataSetWordCount")
      .master("local[2]")
//      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()

    import spark.implicits._
    val data = spark.read.text("data/wordcount.txt").as[String]
    val words = data.flatMap(value => value.split(","))
    val groupedWords = words.groupByKey(_.toLowerCase())
    val counts = groupedWords.count()
    counts.show()


  }
}
