package Moudle.Spark1.sxt.cong.suanfa

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/6/27.
  */
object AccumulateValue {

  // 累加器中的全局变量只能由driver端读取，task只管干活，不能读如果用.value方法的话
  // 累加器如果你写到了Transformation这种转换算子内部的话呢，要想让这个累加器生效，必须
  // 在这个Transformation操作之前的有action操作！！！！例子如下：
  // val accum = sc.accumulator(0)
  // data.map { x => accum += x; f(x) }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AccumulateValue").setMaster("local")
    val sc = new SparkContext(conf)

    val sum = sc.accumulator(0,"Our Accumulator")

    val list = Array(1,2,3,4,5)
    val listRDD = sc.parallelize(list)

    listRDD.foreach(value => {sum+=value; print(sum.value)})
    println(sum.value)
  }
}
