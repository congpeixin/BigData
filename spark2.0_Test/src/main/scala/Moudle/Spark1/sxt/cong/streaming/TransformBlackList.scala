//package Moudle.Spark1.sxt.cong.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Durations, StreamingContext}
//
///**
//  * Created by Administrator on 2016/6/17.
//  */
//object TransformBlackList {
//
//
//    def main(args: Array[String]) {
//        go
//    }
//    def go: Unit ={
//        val conf=new SparkConf().setAppName("bl").setMaster("local[3]")
//        val context=new StreamingContext(conf,Durations.seconds(10))
//        var bl=List[(String,Boolean)]()
//        bl=bl.::(("abc",false))
//        val blRDD=context.sparkContext.parallelize(bl)
//        val logs=context.socketTextStream("mu4",12122)
//        // 所以要先对输入的数据进行转换操作变成 (username, date username) 以便后面对于数据流中的RDD和定义好的黑名单RDD进行join操作
//        val logsMapRDD=logs.map(x=>(x.split(" ")(0),x))
//        val rsDStream=logsMapRDD.transform(x=>{
//            val joinedRDD=x.leftOuterJoin(blRDD)
//            joinedRDD.filter(x=>{
//               !x._2._2.getOrElse(false).toString.toBoolean
//            }).map(x=>x._2._1)
//        })
//        rsDStream.print()
//        context.start()
//        context.awaitTermination()
//    }
//}
