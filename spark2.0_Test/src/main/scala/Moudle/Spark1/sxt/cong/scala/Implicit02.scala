package Moudle.Spark1.sxt.cong.scala

/**
使用隐式转换加强现有类型
  超人变身（装饰类型）
  类型没有方法的时候会尝试隐式转换
 */

class Man(val name:String)

class Superman(val name:String){
  def emitLaser = println("emit a pingpang ball!")
}

object Implicit02 {
  implicit def man2superman(man:Man):Superman = new Superman(man.name)

  def main(args: Array[String]) {
    val yasaka = new Man("yasaka")
    yasaka.emitLaser
  }
}
