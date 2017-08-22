//package com.cong.sparkSQL
//
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by root on 2016/6/27.
//  */
//object BroadCastValue {
//
//  // 定义的全局的广播变量只能在driver改！task不能改只能读！为了安全，每个分布式里面的task都获取到一样的值！
//
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("BroadCastValue").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val f = 3
//    val broadCastFactor = sc.broadcast(f)
//    val list = List(1,2,3,4,5)
//    val listRDD = sc.parallelize(list)
//    val results = listRDD.map( num => num * broadCastFactor.value)
//    results.foreach(results => print(results + "\t"))
//  }
//}
