package Moudle.Spark1.sxt.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cluster on 2017/7/13.
  */
object Socket {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    //创建
    val conf = new SparkConf().setMaster("local[2]").setAppName("Socket")
    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream("192.168.31.105",9999)
    val result = lines.flatMap(_.split(" ")).map((_,1))

    result.foreachRDD(_.foreach(println(_)))
  }
}
