package Moudle.Spark1.sxt.suanfa

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/6/20.
  * 分组取topN
  */
object GroupTopN {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupTopN").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("score.txt")
    val groupedLines = lines.map(x=>(x.split(" ")(0),x.split(" ")(1))).groupByKey()
    //groupedLines.foreach(word => println(word))
    val topList = groupedLines.map(x=>{
      var t = List[Int]()
      for(temp <- x._2){
        t = temp.toInt :: t
      }
      (x._1,t.sortBy(x=> -x).take(2))
    })
    topList.foreach(println(_))
  }
}
