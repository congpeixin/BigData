//package Moudle.Spark1.sxt.scala
//
//object TestFunc {
//
//	def sayMyName(name : String = "Jack"){
//
//	}
//
//
////	def sumMoreParameters(elem : Int*) = {
////	  var sum = 0
////		for(e <- elem){
////			println(e)
////			sum += e
////		}
////	  sum
////	}
//
////
////	def add(a:Int,b:Int) = a+b
////
////
////	def add2  = add(_:Int,2)
//
//
//
////	f(n)=f(n)*f(n-1)
////	def fac(n:Int):Int = if( n <= 0) 1 else n*fac(n-1)
//
//
////	函数柯里化
//	// 柯里化,柯里化比较有意思,就是说定义
//	// 把这个参数一个个独立开来写,这就是柯里化,它可以达到什么效果呢
////	def mulitply(x:Int)(y:Int) = x*y
////
////	def m2 = mulitply(2)_
//	// 同学们可能会说，这个看起来没有什么用处啊，但是我们说柯里化在递归，和控制抽象方面比较有用
////	在目前我们不去深究这个使用场景，现在就是让大家知道有这么个形式，大家掌握到这就可以了
////	柯里化就是把参数可以分开来，把部分函数参数可以用下划线来代替
//
//
//
////	=> 匿名函数声明方式
//    val t = ()=>{333;"xururyun"}//声明了一个函数对象付给了t
//
////    :后面是数据类型,c代表传进来的参数
//    def testfunc02(c : ()=>String ){
//      c()
//    }
//
////	匿名函数
//	val d1 = (a:Int)=> a+100
//
////	匿名函数作为参数,其实就是参数名,后面跟上参数类型,然后是表达式
//    def testf1(callback : (Int,Int)=>Int )={
//    	println(callback(123,123))
//    }
//
//
////    嵌套函数,其实就是def里面套一个def
//	def add3(x:Int, y:Int ,z:Int) = {
//		def add2(x:Int, y:Int) = {
//			x + y
//		}
//		add2(add2(x,y),z)
//	}
//
//
////  能否看懂？？？
//    def sum(f : Int => Int) : (Int , Int) => Int = {
//    		def sumF(a : Int , b : Int) : Int =
//    		  if (a >b ) 0 else f(a) + sumF( a + 1 , b)
//    		sumF
//    }
//
////	  def sum(f : Int => Int) : (Int , Int) => Int = {
////			  def sumF(a : Int , b : Int) : Int ={
////					  if (a >b ) 0 else f(a) + sumF( a + 1 , b)
////			  }
////			  return sumF
////	  }
//
//	def main(args: Array[String]) {
//		var bb = (a: Int) => a+100
//		println(bb(1))
//
////		sayMyName()
////		println(sumMoreParameters(1,2,3,4,5,6,7,8,9,10))
////	    println(add(2,3))
////		  println(add2(3))
//
////		println(fac(5))
////		println(d1(4))
//
////    	println(t)
////    	println(t())
//
////	    println(testf1((x:Int, y:Int)=>{x*100-y*1000}))
//
////		 println( mulitply(123)(123))
////		 println( m2(123))
//
////		println(testfunc02(t))
//
////	  println(testf1((a:Int, b:Int)=>{a+b}))
//
////		testf1(add)
//
////		println(add3(1,2,3))
//
////	    def sum(f :Int => Int) : (Int , Int) => Int = {
////			  def sumF(a : Int , b : Int) : Int ={
////					  if (a >b ) 0 else f(a) + sumF( a + 1 , b)
////			  }
////			  return sumF
////	    }
//
////	  val tttt = (a:Int,b:Int)=>a*b;
////	  testf1(tttt);
////	  testf1((a:Int,b:Int)=>a*b)
//
//
//	}
//}