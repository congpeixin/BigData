package Moudle.Spark1.sxt.scala

object TestMap {
	def main(args: Array[String]) {
	  
//	  _ 通配符  =>匿名函数   <- for便利符号
	  
	  // mutable
	  // immutable
		var m1 =  scala.collection.mutable.Map[String,Int](("a" -> 1), ("b" -> 2))
		
		println(m1("a"))
		m1 += ("c" -> 3)
		println(m1)
		m1.foreach(tuple=>{
	    	println(tuple+" "+tuple._1+" "+tuple._2)
		})

 		m1.keys.foreach(key=>println(m1(key) = 2))

		println(m1)
	}
}