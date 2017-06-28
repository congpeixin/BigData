//package com.cong.suanfa
//
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
//	* Created by root on 2016/8/28.
//	*/
//object GroupByKeyOperator_scala {
//	def main(args: Array[String]) {
//		val conf = new SparkConf().setAppName("LineCount").setMaster("local")
//		val sc = new SparkContext(conf)
//
//		val list = Map("A"->1,"B"->2,"A"->2,"C"->1)
//
//		val result = list.groupBy(String, List<Integer> )
//
//		result.foreach(println)
//
//	}
//}
