package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
	* Created by root on 2016/8/31.
	* Top前三
	*/
object TopN_scala {
	def main(args: Array[String]) {
		/**
			* 单一列取前3
			*/
		val conf = new SparkConf().setAppName("map").setMaster("local")
		var sc = new SparkContext(conf)
		val line = sc.textFile("top3.txt")
//		val key = line.map(num => (num.toInt, num))
//		//key.foreach(tuple => println(tuple))
//		val sortkey = key.sortByKey()
//		val result = sortkey.take(3)
//		result.foreach(tuple => println(tuple._2))


		/**
			* 多列属性取前三
			*/
		//文本行，提取其中的第一个字串作为key，将整个句子作为value，建立 PairRDD
		val kv = line.map(x =>(x.split(" ")(0).toInt,x))
		val result = kv.sortByKey().take(3)
		result.foreach(tuple => println(tuple._2))


	}
}