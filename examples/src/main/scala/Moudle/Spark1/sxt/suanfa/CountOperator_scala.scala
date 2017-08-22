package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object CountOperator_scala {
	def main(args: Array[String]) {


		val list = List(1,2,3,4,5,6,7,8,9,10)
		val result = list.count((int: Int)=>true)
		println(result)
	}
}
