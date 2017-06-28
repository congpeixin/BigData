//package com.cong.sparkSQL
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
//	* Created by root on 2016/9/1.
//	*/
//object DingDan {
//	def main(args: Array[String]) {
//		val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local")
//		val sc = new SparkContext(conf)
//		val sqlContext = new SQLContext(sc)
//
//		case class DateInfo(dateID:String,theyearmonth :String,theyear:String,themonth:String,thedate :String,theweek:String,theweeks:String,thequot :String,thetenday:String,thehalfmonth:String)
//		//对应Stock.txt
//		case class StockInfo(ordernumber:String,locationid :String,dateID:String)
//		//对应StockDetail.txt
//		case class StockDetailInfo(ordernumber:String,rownum :Int,itemid:String,qty:Int,price:Double,amount:Double)
//
//		//加载数据并转换成DataFrame
//		val DateInfoRDD = sc.textFile("/data/Date.txt").map(_.split(",")).map(d => DateInfo(d(0), d(1),d(2),d(3),d(4),d(5),d(6),d(7),d(8),d(9))).toJavaRDD()
//		//加载数据并转换成DataFram
//		val StockInfoRDD= sc.textFile("/data/Stock.txt").map(_.split(",")).map(s => StockInfo(s(0), s(1),s(2))).toJavaRDD()
//		//加载数据并转换成DataFrame
//		val StockDetailInfoRDD = sc.textFile("/data/StockDetail.txt").map(_.split(",")).map(s => StockDetailInfo(s(0), s(1).trim.toInt,s(2),s(3).trim.toInt,s(4).trim.toDouble,s(5).trim.toDouble)).toJavaRDD()
//
//		val DateInfoDF = sqlContext.read.
//
//	}
//}
