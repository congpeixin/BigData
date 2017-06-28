package Moudle.Spark1.sxt.cong.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object FlatMapOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")
		val sc = new SparkContext(conf)

		val list = List("cong pei xin","hu yu ting ","zhang chuang")
		val result = list.flatMap(list => list.split(" "))
		result.foreach(word => println(word))

	}
}
