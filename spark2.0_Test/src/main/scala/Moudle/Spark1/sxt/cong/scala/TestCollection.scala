//package com.cong.scala
//
//object TestCollection {
//	def main(args: Array[String]) {
//
//		var t = List(1,2,3,5,5)
//		println("---001--"+t(2))
//
////		map 个位置相加  函数编程
//		println(t.map(a => {print("***"+a); a+2}));
//		println(t.map(_ + 2))
//
//		var t2 = t.+:("test")//添加元素
////		println(t.::("test"))
////		println("test"::t)
////		println(6::t2)
////		println(t2.::(6))
////		println(List(6).:::(t2))
////		println((t2):::List(6))
////		println(t2)
////		t2 = t:::6::Nil //组成新的List  t作为一个元素
////		println(t2)
//
//		t2.foreach(a=>print("---+++"+a))
//
//		println("/--***---"+t.distinct)
//
//		println(t.sortBy{ x => -x })
//
//		println("---+++++********************Slice"+t.slice(0, 2))
//
////		println("-*--*--*--*--*--*--*--*--*-")
////		for(temp<-t2){
////		  print(temp)
////		}
//
////		println("-*--*--*--*--*--*--*--*--*-")
////		for(i <- 0 until t2.length){
////		  println(i)
////		  println(t2(i))
////		}
//
//
//		println("-*--*--*--*--*--*--*--*--*-")
//		println(t./:(0)({
//		    (sum,num)=>sum-num
//		}))
//
//		// 1,2,3,5,5
//		println(t.reduce( _-_ ))
//		println(t.filter ( _>3 ))
//		println(t.take(3))
//
////		println("-*--*--*--*--*--*--*--*--*-")
////		println(t.foldLeft(0)((sum,num)=>{
////		  print(sum+"--"+num+" ");
////			sum-num;
////		}));
//
////		println("-*--*--*--*--*--*--*--*--*-")
////		println(t.map(v =>v+2));
//
////		println("-*--*--*--*--*--*--*--*--*-")
//////		元组
////		var tuple01 = (1,5,6,6)
////		println(tuple01._1)
////		println(tuple01._4)
////
////		var list = List(("yasaka",100),("laoxiao",100),("xiaochen",50))
////		val temp = list.sortBy(x => (-x._2,x._1))
////		println(temp)
//	}
//}