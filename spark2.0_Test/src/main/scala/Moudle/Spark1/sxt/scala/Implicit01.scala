package Moudle.Spark1.sxt.scala

/*
隐式转换
特殊售票窗口（只接受特殊人群，比如学生、老人）
类型不对会尝试进行隐式转换
 */
class SpecialPerson(val name:String)
class Student(val name:String)
class Older(val name:String)

class Teacher(val name:String)

object Implicit01 {

  implicit def object2SpecialPerson(obj:Object): SpecialPerson = {
    if(obj.getClass == classOf[Student]){
      val stu = obj.asInstanceOf[Student]
      new SpecialPerson(stu.name)
    }else if (obj.getClass == classOf[Older]){
      val older = obj.asInstanceOf[Older]
      new SpecialPerson(older.name)
    }else{
      Nil
    }
  }

  var ticketNumber = 0
  
  def buySpecialTicket(p:SpecialPerson)={
    ticketNumber += 1
    "T-" + ticketNumber
  }

  def main(args: Array[String]) {
    val zhangyuanyuan = new Student("zhangyuanyuan")
    println(buySpecialTicket(zhangyuanyuan))
    val gaga = new Older("gaga")
    println(buySpecialTicket(gaga))
    val yasaka = new Teacher("yasaka")
    println(buySpecialTicket(yasaka))
  }
}
