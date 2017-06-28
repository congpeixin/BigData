//package com.cong.sparkSQL
//
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
//import org.apache.spark.sql.types._
//
///**
//  * Created by root on 2016/8/8.
//  */
//class StringGroupCount extends UserDefinedAggregateFunction{
//
//  // 输入数据的类型
//  def inputSchema: StructType = {
//    StructType(Array(StructField("str", StringType, true)))
//  }
//
//  // 中间结果的类型
//  def bufferSchema: StructType = {
//    StructType(Array(StructField("count", IntegerType, true)))
//  }
//
//  // 还需要最后的数据类型
//  def dataType : DataType = {
//    IntegerType
//  }
//
//  def deterministic: Boolean = {
//    true
//  }
//
//  // 初始值
//  def initialize(buffer: MutableAggregationBuffer): Unit = {
//    buffer.update(0, 0l)
//    buffer.update(1, 0d)
//  }
//
//  // 局部累加
//  def update(buffer : MutableAggregationBuffer, input : Row):Unit = {
//    buffer.update(0, buffer.getAs[Long](0) + 1) //条数加1
//    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0)) //输入汇总
//  }
//
//  // 全局累加
//  def merge(buffer:MutableAggregationBuffer, bufferx: Row): Unit ={
//    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
//    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))
//  }
//
//  // 最后有一个方法可以更改你返回的数据样子
//  def evaluate(buffer: Row) : Any = {
//    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
//    f"$avg%.2f".toDouble
//  }
//}
