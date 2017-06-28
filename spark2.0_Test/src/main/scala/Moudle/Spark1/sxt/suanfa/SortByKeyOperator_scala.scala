package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object SortByKeyOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		var sc = new SparkContext(conf)
		val student = sc.parallelize(Array((2, "李欣"), (4, "张良"),(1, "丛培欣"),(3, "张闯")))
		val name = sc.parallelize(Array(("Baby",10), ("Dube",10),("Angle",10),("LadyGAGA",10)))

		val result = student.sortByKey().foreach(word => println(word))
		val result1 = name.sortByKey().foreach(word => println(word))
	}
}
