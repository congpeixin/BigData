//package Moudle.Spark2.streaming
//
//import org.apache.spark.sql.SparkSession
//
///**
// *
// * Example:
// *    `$ bin/run-example \
// *      sql.streaming.StructuredKafkaWordCount host1:port1,host2:port2 \
// *      subscribe topic1,topic2`
// */
//object StructuredKafkaWordCount {
//  def main(args: Array[String]): Unit = {
//    if (args.length < 3) {
//      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
//        "<subscribe-type> <topics>")
//      System.exit(1)
//    }
//
//    val Array(bootstrapServers, subscribeType, topics) = args
//
//    val spark = SparkSession
//      .builder
//      .appName("StructuredKafkaWordCount")
//      .getOrCreate()
//
//    // Create DataSet representing the stream of input lines from kafka
//    val lines = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", bootstrapServers)
//      .option(subscribeType, topics)
//      .load()
//      .selectExpr("CAST(value AS STRING)")
//      .as[String]
//
//    // Generate running word count
//    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
//
//    // Start running the query that prints the running counts to the console
//    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//
//    query.awaitTermination()
//  }
//
//}
//// scalastyle:on println