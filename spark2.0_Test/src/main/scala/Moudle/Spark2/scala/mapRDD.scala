package Moudle.Spark2.scala

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2017/6/17.
  */
object mapRDD {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class User(id: Int,name: String,pwd: String,sex: Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

    val mapRDD = spark.sparkContext.textFile("data/file1.txt")
    val mapRDD1 = mapRDD.map(row => row.split(","))

    mapRDD1.collect().foreach(iterator => {
            println(iterator.toIterable)

    })


    val datas: Array[String] = Array(
      "{'id':1,'name':'xl1','pwd':'xl123','sex':2}",
      "{'id':2,'name':'xl2','pwd':'xl123','sex':1}",
      "{'id':3,'name':'xl3','pwd':'xl123','sex':2}")

    val mapRDD2 = spark.sparkContext.makeRDD(datas).map(data =>{new Gson().fromJson(data, classOf[User])}).foreach(user =>println("id: " + user.id
      + " name: " + user.name
      + " pwd: " + user.pwd
      + " sex:" + user.sex))





    spark.stop()


  }
}
