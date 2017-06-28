package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object CountByKeyOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		var sc = new SparkContext(conf)
		val list = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1)))

		val result = list.countByKey()

		result.foreach(word => println(word))
	}
}
