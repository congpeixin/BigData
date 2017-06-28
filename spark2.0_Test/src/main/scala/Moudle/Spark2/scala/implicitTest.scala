package Moudle.Spark2.scala

/**
  * Created by cluster on 2017/3/14.
  */
object implicitTest {
  def foo(msg : String) = println(msg)
  implicit def intToString(int: Int):String = int.toString
  def main(args: Array[String]) {
    foo(10)
  }
}
