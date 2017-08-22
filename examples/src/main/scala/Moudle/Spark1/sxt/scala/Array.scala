package Moudle.Spark1.sxt.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/9/30.
	*/
object Array {

	def main(args: Array[String]) {
//		var words = List("cong","pei","xin")
//		val count = 10
//
//		val result = words.map(x => (x,count))
//
//		result.foreach(y => println(y))
    var conf = new SparkConf().setAppName("wordcount").setMaster("local");
		var sc = new SparkContext(conf)

		val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2)))
		val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
		val rdd3 = rdd1.join(rdd2)
		rdd3.foreach(y => println(y))
	}
}
