package Moudle.Spark1.sxt.cong.sparkSQL.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/6/13.
  */
object MapPartitionsOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapPartitionsOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val arr = Array("xurunyun","liangyongqi","wangfei")
    val rdd = sc.parallelize(arr)

    val result = rdd.mapPartitions(x => x)
    result.foreach(println(_))
  }
}
