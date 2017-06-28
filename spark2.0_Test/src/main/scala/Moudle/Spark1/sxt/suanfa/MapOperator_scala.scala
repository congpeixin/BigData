package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object MapOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		var sc = new SparkContext(conf)
		val list = List(1, 2, 3, 4, 5)
//		val result = list.map(word => word * 10)
//		result.foreach(word => println(word))
    val result = list.map(_* 10)
		result.foreach(word => println(word))
	}
}
