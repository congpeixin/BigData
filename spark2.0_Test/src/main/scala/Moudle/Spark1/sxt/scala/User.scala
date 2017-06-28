package Moudle.Spark1.sxt.scala

class User (x:Int,age:Int){//默认构造器
	
  val height = x
	val this.age = age
	var name =""
	
	def this(){
		this(5,5)
	}
  
	def this(t1:Int,t2:Int,name:String){
		this(5,5)
		this.name = name
	}
}