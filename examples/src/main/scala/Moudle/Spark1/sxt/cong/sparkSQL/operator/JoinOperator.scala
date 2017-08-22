package Moudle.Spark1.sxt.cong.sparkSQL.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yasaka on 2016/6/8.
  */
object JoinOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JoinOperator")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val nameList = Array(Tuple2(1,"xuruyun"),Tuple2(2,"liangyongqi"),Tuple2(3,"wangfei"))
    val scoreList = Array(Tuple2(1,150),Tuple2(2,100),Tuple2(3,90))
    val name = sc.parallelize(nameList)
    val score = sc.parallelize(scoreList)
    val results = name.join(score)

    results.foreach(println(_))

//    val temp = results.collect()
//    for(t <- temp){
//      println(t)
//    }
  }
}
