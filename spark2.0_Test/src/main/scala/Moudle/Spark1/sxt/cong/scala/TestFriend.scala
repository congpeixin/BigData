package Moudle.Spark1.sxt.cong.scala

trait Listen {
  val name: String
  def listen() = {
    println("You friend " + name + " is listening")
  }
}

trait Read {
  val name: String
  def read() = {
    println("You friend " + name + " is reading")
  }
}

trait Speak {
  val name: String
  def speak() = {
    println("You friend " + name + " is speaking.")
  }
}

class Human(val name: String) {
  def listen() = {
    println(name + " is listening.")
  }
}

class Animal(val name: String)

class Cat(override val name: String) extends Animal(name: String) with Speak with Listen with Read {
  override def toString(): String = " hello " + name + "! "
}

object TestFriend {
  def main(args: Array[String]) {
//        		val f1 = new Listen();//java 接口很像 不能直接够造

        val h1 = new Human("gaga")
    
        h1.listen()
        
        val h2 = new Human("xuruyun") with Speak
        h2.speak()

    //这样scat就是Friend了
    val scat = new Cat("hello kitty")
    scat.listen()
    scat.speak()
    println(scat)
  }
}