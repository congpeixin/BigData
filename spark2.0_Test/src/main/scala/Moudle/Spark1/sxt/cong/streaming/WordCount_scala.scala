package Moudle.Spark1.sxt.cong.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object TCPWordCount {
	def main(args: Array[String]) {
		//setMaster("local[2]")本地执行2个线程，一个用来接收消息，一个用来计算
		val conf = new SparkConf().setMaster("local[2]").setAppName("TCPWordCount")
		//创建spark的streaming,传入间隔多长时间处理一次，间隔在5秒左右，否则打印控制台信息会被冲掉
		val scc =new StreamingContext(conf,Seconds(5))
		//读取数据的地址：从某个ip和端口收集数据
		val lines = scc.socketTextStream("node1", 8888)
		//进行rdd处理
		val results = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
		//将结果打印控制台
		results.print()
		//results.saveAsTextFiles("hdfs://node1:8020/home/wordcount0903")
		//启动spark streaming
		scc.start()
		//等待终止
		scc.awaitTermination()
	}
}
