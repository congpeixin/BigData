//// scalastyle:off println
//package Moudle.Spark2.streaming
//
//import java.sql.Timestamp
//
//import org.apache.spark.sql.SparkSession
//
///**
// *
// * To run this on your local machine, you need to first run a Netcat server
// *    `$ nc -lk 9999`
// * and then run the example
// *    `$ bin/run-example sql.streaming.StructuredNetworkWordCountWindowed
// *    localhost 9999 <window duration in seconds> [<slide duration in seconds>]`
// *
// * One recommended <window duration>, <slide duration> pair is 10, 5
// */
//object StructuredNetworkWordCountWindowed {
//
//  def main(args: Array[String]) {
//    if (args.length < 3) {
//      System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
//        " <window duration in seconds> [<slide duration in seconds>]")
//      System.exit(1)
//    }
//
//    val host = args(0)
//    val port = args(1).toInt
//    val windowSize = args(2).toInt
//    val slideSize = if (args.length == 3) windowSize else args(3).toInt
//    if (slideSize > windowSize) {
//      System.err.println("<slide duration> must be less than or equal to <window duration>")
//    }
//    //scala 中的 s 函数，返回字符串
//    val windowDuration = s"$windowSize seconds"
//    val slideDuration = s"$slideSize seconds"
//
//    val spark = SparkSession
//      .builder
//      .appName("StructuredNetworkWordCountWindowed")
//      .getOrCreate()
//
//    // Create DataFrame representing the stream of input lines from connection to host:port
//    val lines = spark.readStream
//      .format("socket")
//      .option("host", host)
//      .option("port", port)
//      .option("includeTimestamp", true)
//      .load()
//
//    // Split the lines into words, retaining timestamps
//    val words = lines.as[(String, Timestamp)].flatMap(line =>
//      line._1.split(" ").map(word => (word, line._2))
//    ).toDF("word", "timestamp")
//
//    // Group the data by window and word and compute the count of each group
//    val windowedCounts = words.groupBy(
//      window($"timestamp", windowDuration, slideDuration), $"word"
//    ).count().orderBy("window")
//
//    // Start running the query that prints the windowed word counts to the console
//    val query = windowedCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", "false")
//      .start()
//
//    query.awaitTermination()
//  }
//}
//// scalastyle:on println
