package Moudle.Spark1.sxt.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/27.
	*/
object WordCount_1 {
	def main(args: Array[String]) {
		var conf = new SparkConf().setAppName("wordcount").setMaster("local");
		var sc = new SparkContext(conf)
		var line = sc.textFile("word")
		//打印读取文件后格式是什么样子的
		//var result = line.foreach(word => println(word))
		var words = line.flatMap(_.split(" "))
		//打印显示分割后的单词
		//var result = words.foreach(word => println(word))
		var pairs = words.map((_,1))
		//打印将单词转换成key,value的形式
		//var result = pairs.foreach(word => println(word))
		var kv = pairs.reduceByKey((_+_))
		//按照value进行排序
		var sk = kv.sortBy(_._2,false)
		//按照key排序
		var sv = kv.sortByKey()
		//打印结果
		var result = sk.foreach(word => println(word))
	}
}
