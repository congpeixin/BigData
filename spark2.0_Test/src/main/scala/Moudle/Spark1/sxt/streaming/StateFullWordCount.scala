package Moudle.Spark1.sxt.streaming

/**
	* Created by root on 2016/9/3.
	*/
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.HashPartitioner
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.slf4j.LoggerFactory
object StateFullWordCount {
	/**
		* String:某个单词
		* Seq：[1,1,1,1,1,1]，当前批次出现的次数的序列
		* Option:历史的结果的sum
		*/

	val updateFunction = (iter: Iterator[(String,Seq[Int],Option[Int])]) => {
		//将iter中的历史次数和现有的数据叠加，然后将单词和出现的最后次数输出
		//iter.flatMap(t=>Some(t._2.sum + t._3.getOrElse(0)).map(x=>(t._1,x)))
		iter.flatMap{case(x,y,z)=>Some(y.sum+z.getOrElse(0)).map(v =>(x,v))}
	}
	def main(args: Array[String]) {
		//设置日志级别
		Logger.getRootLogger.setLevel(Level.WARN)
		//创建
		val conf = new SparkConf().setMaster("local[2]").setAppName("StateFullWordCount")
		val ssc = new StreamingContext(conf,Seconds(5))
		//回滚点设置在本地
		//ssc.checkpoint("./")
		//将回滚点写到hdfs
		//ssc.checkpoint("hdfs://node1:8020/home")
		val lines = ssc.socketTextStream("node1", 8888)

		/**
			* updateStateByKey()更新数据
			* 1、更新数据的具体实现函数
			* 2、分区信息
			* 3、boolean值
			*/
		val results = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunction,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
		results.print()
		ssc.start()

		ssc.awaitTermination()
	}
}
