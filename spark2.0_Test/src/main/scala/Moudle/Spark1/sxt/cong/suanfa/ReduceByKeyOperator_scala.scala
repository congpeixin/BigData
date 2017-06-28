package Moudle.Spark1.sxt.cong.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/28.
	*/
object ReduceByKeyOperator_scala {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		var sc = new SparkContext(conf)
		val list = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1)))
		val result = list.reduceByKey((x,y)=>(x+y))
		result.foreach(kv=>println(kv))
	}
}
