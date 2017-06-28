package Moudle.Spark1.sxt.scala

import org.apache.spark.{SparkContext, SparkConf}
/**
	* Created by Cpeixin on 2016/8/27.
	*/
object WordCount_2 {
	def main(args: Array[String]) {
		var conf = new SparkConf().setAppName("wordcount").setMaster("local");
		var sc = new SparkContext(conf).textFile("word").flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_)).sortBy(_._2,false).foreach(word => println(word))
	}
}
