package Moudle.Spark2.DataSet

import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2018/1/23.
  */
object DS_transformations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DatasetOps")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "/spark-warehouse")
      .getOrCreate()

    val textFile = spark.read.textFile("examples/src/main/scala/Moudle/textfile")
    import spark.implicits._
//    val maxNum = textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
//    val wordCount = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count().collect()
    val wordCount = textFile.rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    println(wordCount.toDebugString)
  }
}
