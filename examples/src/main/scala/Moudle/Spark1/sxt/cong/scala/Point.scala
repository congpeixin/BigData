package Moudle.Spark1.sxt.cong.scala

class Point(val x: Int, val y: Int) {
	var t = x;
	var t1 = y;
	
	val isOriginal: Boolean = {
			x == 0 && y == 0
	}
	
	def this(xArg: Int) {
		this(xArg, 12344)
		println("hello, I'm this constructor")
	}
}

object Point {
  
  def main(args: Array[String]) {
    val p1 = new Point(123)
    println(p1.x)
    val p2 = new Point(555, 777)
    println(p2.x)
  }
}