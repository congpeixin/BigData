package Moudle.Spark1.sxt.scala

class TestObject {
	val t2 = "lskjdfkljd"
	var t=123
	def func01() = {
		println("gaga");
	}
}

object TestObject {
	val t1 = 123;
	var  ssssgagag=1444;
	val single = new TestObject()
	
	def func02() = {
		println("gaga")
	}
	
	def main(args: Array[String]) {
		val t1 = new TestObject()
		
		println(t1.t2)
		t1.func01()
		
		TestObject.func02()
		println(TestObject.t1)
		println(TestObject.ssssgagag)
	}
}