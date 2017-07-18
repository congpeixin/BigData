package Moudle.Spark2.streaming
// scalastyle:off println
import org.apache.spark.sql.SparkSession

object StreamingSocket {
  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }
    val warehouseLocation = "/spark-warehouse"
    val host = "192.168.31.105"
    val port = "9999"

    val spark = SparkSession
      .builder
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
