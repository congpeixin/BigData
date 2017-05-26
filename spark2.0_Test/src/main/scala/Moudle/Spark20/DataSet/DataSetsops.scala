package Moudle.Spark20.DataSet

/**
  * Created by cluster on 2017/3/14.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataSetsops {
  val log = Logger.getLogger(DataSetsops.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class Person(name: String,age: Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DatasetOps")
      .master("local")
      .config("spark.sql.warehouse.dir", "/spark-warehouse")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val personDF= spark.read.json("D:\\people.json")
    personDF.show()
    println("=======================personDF.show()============================")
    //    +----+-------+
    //    | age|   name|
    //    +----+-------+
    //    |null|Michael|
    //    |  30|   Andy|
    //    |  19| Justin|
    //    +----+-------+

    personDF.collect().foreach (println) //collect() ,返回值是一个数组，返回dataframe集合所有的行
    println("=======================personDF.collect()============================")

    println(personDF.count()) //count(),dataframe的行数
    println("=======================personDF.count()============================")
    /**
      * DataFrame转DataSet 调用 .as[]即可
      */
    val personDS = personDF.as[Person]

    personDS.show()
    println("======================= personDS.show()============================")
    //    +----+-------+
    //    | age|   name|
    //    +----+-------+
    //    |null|Michael|
    //    |  30|   Andy|
    //    |  19| Justin|
    //    +----+-------+

    personDS.printSchema()
    println("=======================personDS.printSchema()============================")
    /**
      * DataSet转DataFrame 调用 .toDF()即可
      */
    val dataframe=personDS.toDF()

    personDF.createOrReplaceTempView("persons")

    spark.sql("select * from persons where age > 20").show()
    println("=======================age > 20\").show()============================")

    spark.sql("select * from persons where age > 20").explain()
    println("=======================age > 20\").explain()============================")

    personDF.filter("age > 20").show()
    println("=======================personDF.filter(\"age > 20\").show()============================")

    val personScoresDF= spark.read.json("D:\\peopleScores.json")
    // personDF.join(personScoresDF,$"name"===$"n").show()
    personDF.filter("age > 20").join(personScoresDF,$"name"===$"n").show()
    println("=======================personDF.filter(\"age > 20\").join(personScoresDF,$\"name\"===$\"n\").show()============================")

    personDF.filter("age > 20")
      .join(personScoresDF,$"name"===$"n")
      .groupBy(personDF("name"))
      .agg(avg(personScoresDF("score")),avg(personDF("age")))
      .show()

    while(true) {}

    spark.stop()
  }
}
