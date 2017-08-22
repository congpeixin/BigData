package Moudle.Spark1.sxt.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/30.
	*/
class WordCount_jar {

}
object WordCount_jar {
	def main(args: Array[String]): Unit = {
		var conf = new SparkConf().setAppName("wordcount")
		var sc = new SparkContext(conf)
		var line = sc.textFile("/usr/word")
		var words = line.flatMap(word => word.split(" "))
		var pairs = words.map(word => (word,1))
		var kv = pairs.reduceByKey((x,y)=>(x+y))
		var sk = kv.sortBy(word => word._2,false)
		var sv = kv.sortByKey()
		var result = sv.foreach(word => println(word))

	}
}