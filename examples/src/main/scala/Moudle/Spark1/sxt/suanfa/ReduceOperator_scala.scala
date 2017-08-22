package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object ReduceOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")
		val sc = new SparkContext(conf)
		val list = List(1, 2, 3, 4, 5)

		val result = list.reduce((num1,num2)=>num1+num2)
		println(result)
	}
}
