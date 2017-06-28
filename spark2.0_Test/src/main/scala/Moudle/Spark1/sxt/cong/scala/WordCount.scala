package Moudle.Spark1.sxt.cong.scala

import org.apache.spark.{SparkContext, SparkConf}
/**
	* Created by root on 2016/8/26.
	*/
class WordCount {

}
object WordCount {
	def main(args: Array[String]): Unit = {
		var conf = new SparkConf().setAppName("wordcount").setMaster("local")
		var sc = new SparkContext(conf)
		var line = sc.textFile("word")
		//打印读取文件后格式是什么样子的
		//var result1 = line.foreach(word => println("1.读取文件（textFile）："+word))
		var words = line.flatMap(word => word.split(" "))
		//打印显示分割后的单词
		//var result2 = words.foreach(word => println("2.分割单词（flatMap） ："+word))
		var pairs = words.map(word => (word,1))
		//打印将单词转换成key,value的形式
		//var result3 = pairs.foreach(word => println("3.转换成键值对（map） ："+word))
		var kv = pairs.reduceByKey((x,y)=>(x+y))
		//按照value进行排序
		//var result4 = kv.foreach(word => println("4.合并Value值（reduceByKey） ："+word))
		var sk = kv.sortBy(word => word._2,false)
		//按照key排序
		//var sv = kv.sortByKey()
		//打印结果
		val top3 = sk.take(3)
		println("5.排序--top3")
		var result = top3.foreach(word => println(word))

	}
}
