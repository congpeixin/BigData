package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object CartesianOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		var sc = new SparkContext(conf)

		val man = sc.parallelize(List("丛培欣","李欣","张闯"))
		val weman = sc.parallelize(List("郑爽","关晓彤","娜扎"))

		val result = man.cartesian(weman)

		result.foreach(pair => println(pair))
	}
}
