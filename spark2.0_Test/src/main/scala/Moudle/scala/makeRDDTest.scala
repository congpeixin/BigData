package Moudle.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2017/6/17.
  * RDD的来源
  * 1.创建RDD
  * 2.外部数据读取RDD
  * 3.其它RDD的转化
  */
object makeRDDTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val list = List(1,2,3,4,5,6)
    val array = Array(2,4,6,8)
    /**
      * def parallelize[T:ClassTag](
      seq:Seq[T],
      numSlices:Int =defaultParallelism):RDD[T]

        def makeRDD[T:ClassTag](
          seq:Seq[T],
          numSlices:Int =defaultParallelism):RDD[T]

      以上两种创建RDD的方式是相同的，接收参数是集合


        def makeRDD[T:ClassTag](seq:Seq[(T, Seq[String])]):RDD[T]
        上面这种创建RDD的方式，提供了RDD里面数据的位置信息
      */

    val makeRDD_1 = spark.sparkContext.makeRDD(1 to 10)
    val makeRDD_2 = spark.sparkContext.makeRDD(list)
    val makeRDD_3 = spark.sparkContext.makeRDD(array)
    makeRDD_3.foreach(println)

    spark.stop()


  }
}
