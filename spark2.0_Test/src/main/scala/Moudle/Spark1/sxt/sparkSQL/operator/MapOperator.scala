package Moudle.Spark1.sxt.sparkSQL.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/6/7.
  */
object MapOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5)
    val numberRDD = sc.parallelize(numbers)
    val results = numberRDD.map(_ * 10)
    results.foreach(println(_))
  }
}
