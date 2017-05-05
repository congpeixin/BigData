package com.spark2Test.study.DataSet

import org.apache.spark.sql.SparkSession

/**
  * Created by cluster on 2017/3/15.
  */
object DataSetsops_2 {
  case class Person(name:String,age:Long)
  case class Score(n:String,score:Long)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DatasetOps")
      .master("local")
      .config("spark.sql.warehouse.dir", "/spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //people.json 格式
//    {"name":"Michael", "age":16}
//    {"name":"Andy", "age":30}
//    {"name":"Justin", "age":19}
//    {"name":"Justin", "age":29}
//    {"name":"Michael", "age":46}
    val personDF= spark.read.json("D:\\people.json")
    val personScoresDF= spark.read.json("D:\\peopleScores.json")
    val personDS = personDF.as[Person]
    val personScoresDS =personScoresDF.as[Score]


//    personDS.groupBy($"name").agg(sum($"age"),avg($"age"),max($"age"),min($"age")
//      ,countDistinct($"age"),mean($"age"),current_date()).show
//    +-------+--------+--------+--------+--------+-------------------+--------+--------------+
//    |   name|sum(age)|avg(age)|max(age)|min(age)|count(DISTINCT age)|avg(age)|current_date()|
//    +-------+--------+--------+--------+--------+-------------------+--------+--------------+
//    |Michael|      62|    31.0|      46|      16|                  2|    31.0|    2017-03-15|
//    |   Andy|      30|    30.0|      30|      30|                  1|    30.0|    2017-03-15|
//    | Justin|      48|    24.0|      29|      19|                  2|    24.0|    2017-03-15|
//    +-------+--------+--------+--------+--------+-------------------+--------+--------------+

    personDS.groupBy($"name",$"age").agg(concat($"name", $"age")).show()
//    +-------+---+-----------------+
//    |   name|age|concat(name, age)|
//    +-------+---+-----------------+
//    | Justin| 29|         Justin29|
//    |   Andy| 30|           Andy30|
//    |Michael| 16|        Michael16|
//    |Michael| 46|        Michael46|
//    | Justin| 19|         Justin19|
//    +-------+---+-----------------+
//
    personDS.groupBy($"name")
      .agg(collect_list($"name"),collect_set($"name"))
      .collect().foreach { println }
//    [Michael,WrappedArray(Michael, Michael),WrappedArray(Michael)]
//    [Andy,WrappedArray(Andy),WrappedArray(Andy)]
//    [Justin,WrappedArray(Justin, Justin),WrappedArray(Justin)]

    spark.stop()
  }
}
