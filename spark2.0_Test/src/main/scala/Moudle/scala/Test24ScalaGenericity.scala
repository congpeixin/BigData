package Moudle.scala

/**
  * Created by cluster on 2017/7/13.
  */
class Test24ScalaGenericity[T, S](val first: T, val second: S) {
// 泛型方法
def compute[W](list: List[W]) = list.foreach(x => print(x + "\t"))
}

object Test24ScalaGenericity {
  def main(args: Array[String]) {
    val t = new Test24ScalaGenericity[Int, String](1, "片刻")
    println(t.first + " : " + t.second)

    t.compute[Int](List(1, 2, 3))
  }
}
