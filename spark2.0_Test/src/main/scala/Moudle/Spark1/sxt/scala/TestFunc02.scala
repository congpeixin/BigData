package Moudle.Spark1.sxt.scala

object TestFunc02 {
	
  def  test() : Unit = {
	  for(i <- 1.to(100)){
		  println(i)
	  }
//	  for(i <- 1 to 100 ){
//		  println(i)
//	  }
	}
	
	def  test2() = {
			for(i <- 1 until 100 ){
				println(i)
			} 
	}
	
	def  test3() = {
		for(i <- 0 to 100 if (i % 2) == 1 ; if (i % 5) > 3 ){
		  println("I: "+i)
		}
	}
	  
//	switch
  def testmatch(n:Any)={
    n match {
      case x:Int => {println("111") ;n}
//    	break;
    	case s:String => println("2222") ;n
    	case _ => println("other"); "test"//default
    }
  }
	
  def sum(elem : Int*) = {
    var temp = 0
    for(e <- elem){
      temp += e
    }
    temp
  }

	def main(args: Array[String]) {
//		test()
//		test2()
//		test3()
//		println(testmatch(100))
	  
//	  println(sum(1,2,3,4,5))
//	  println(1 to 5)
	  println(sum(1 to 5 :_*))
	}
}