package Moudle.Spark1.sxt.scala

/*
 * 隐式参数
 */
class SignPen{
  def write(content: String) = println(content)
}
object ImplicitContext{
   implicit val signPen = new SignPen
}
object Implicit04 {
  def signForExam(name:String)(implicit signPen : SignPen): Unit ={
    signPen.write(name + " arrive in time.")
  }
  def main(args: Array[String]) {
    import ImplicitContext._
    signForExam("yasaka")
    signForExam("laoxiao")
  }
}
