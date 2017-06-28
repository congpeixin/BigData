package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object CogroupOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		var sc = new SparkContext(conf)
		val student = sc.parallelize(Array((1, "丛培欣"), (2, "李欣"), (3, "张闯"), (4, "张良")))
		val score = sc.parallelize(Array((1, 100), (2, 99), (3, 54), (4, 66)))

		val result = student.cogroup(score)

		result.sortBy(word => word._1,false).foreach(word => println("id :"+ word._1+"name :"+ word._2._1+"score :"+ word._2._2))

	}
}
