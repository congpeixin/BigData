package Moudle.Spark1.sxt.suanfa;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class JoinAndCogroup {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinAndCogroup").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer, String>> studentsList = Arrays.asList(
				new Tuple2<Integer,String>(1,"yasaka"),
				new Tuple2<Integer,String>(2,"xuruyun"),
				new Tuple2<Integer,String>(2,"liangyongqi"),
				new Tuple2<Integer,String>(3,"wangfei")
				);
		
		List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
				new Tuple2<Integer,Integer>(1,100),
				new Tuple2<Integer,Integer>(2,90),
				new Tuple2<Integer,Integer>(3,80),
				new Tuple2<Integer,Integer>(1,101),
				new Tuple2<Integer,Integer>(2,91),
				new Tuple2<Integer,Integer>(3,81),
				new Tuple2<Integer,Integer>(3,71)
				);
		
		JavaPairRDD<Integer,String> studentsRDD = sc.parallelizePairs(studentsList);
		JavaPairRDD<Integer,Integer> scoresRDD = sc.parallelizePairs(scoresList);
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = studentsRDD.join(scoresRDD);
		studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> student)
					throws Exception {
				System.out.println("student id: " + student._1);
				System.out.println("student name: " + student._2._1);
				System.out.println("student score: " + student._2._2);
				System.out.println("===================================");
			}
		});
		
//		JavaPairRDD<Integer,Tuple2<Iterable<String>,Iterable<Integer>>> studentScores = studentsRDD.cogroup(scoresRDD);
//		studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {
//			
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(
//					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> student)
//					throws Exception {
//				System.out.println("student id: " + student._1);
//				System.out.println("student name: " + student._2._1);
//				System.out.println("student score: " + student._2._2);
//				System.out.println("===================================");
//			}
//		});
		
		sc.close();
	}
}
