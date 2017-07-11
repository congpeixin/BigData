package Moudle.Spark2.streaming

import org.apache.spark.sql.SparkSession

/**
  * Created by cluster on 2017/7/11.
  * StructuredStreaming 连接 Kafka
  */

//assign：	json string {“topicA”:[0,1],”topicB”:[2,4]}	用于指定消费的 TopicPartitions，assign，subscribe，subscribePattern 是三种消费方式，只能同时指定一个
//subscribe：	A comma-separated list of topics	用于指定要消费的 topic
//subscribePattern：	Java regex string	使用正则表达式匹配消费的 topic
//kafka.bootstrap.servers：	A comma-separated list of host:port	kafka brokers
object StructuredStreamingKafka {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark structured streaming Kafka example")
      .master("local[2]")
      .getOrCreate()

    val inputstream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "testss")
      .load()
    import spark.implicits._
    val query = inputstream.select($"key", $"value")
      .as[(String, String)].map(kv => kv._1 + " " + kv._2).as[String]
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
