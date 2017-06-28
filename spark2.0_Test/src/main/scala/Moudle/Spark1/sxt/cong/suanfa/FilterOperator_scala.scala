package Moudle.Spark1.sxt.cong.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object FilterOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")
		val sc = new SparkContext(conf)

		val list = List(1,2,3,4,5,6,7,8,9,10)

		val result = list.filter((num: Int) => num % 2 == 0)
		result.foreach(num => println(num))
	}
}
