//package Moudle.Spark1.sxt.sparkSQL;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
//import scala.Tuple2;
//
//public class JSONDataSource {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
//
//		//将json数据读入到 创建的DataFrame
//		DataFrame studentScoresDF = sqlContext.read().json("students.json");
//		// 针对学生成绩信息的DataFrame,注册临时表,查询分数大于80分学生的姓名
//		//创建临时表，是为了下面将执行sql语句
//		studentScoresDF.registerTempTable("student_scores");
//		//查询分数大于80分学生的姓名，将结果存入到goodStudentNamesDF
//		DataFrame goodStudentNamesDF = sqlContext.sql("select name, score from student_scores where score>=80");
//		// 我们接下来把它给转换过来,因为这个时候DataFrame里面的元素还是Row吧,我们把它转换成String
//		List<String> goodStudentNames = goodStudentNamesDF.javaRDD().map(new Function<Row, String>() {
////
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String call(Row row) throws Exception {
//				return row.getString(0);
//			}
//		}).collect();
//
//		// var list = List[String]()
//		// list.::(a)
//		List<String> studentInfoJSONs = new ArrayList<String>();
//		studentInfoJSONs.add("{\"name\":\"Cpeixin\", \"age\":22}");
//		studentInfoJSONs.add("{\"name\":\"Nazha\", \"age\":23}");
//		studentInfoJSONs.add("{\"name\":\"Hyuting\", \"age\":24}");
//		studentInfoJSONs.add("\"name\":\"Zchuang\", \"age\":25");
//
//		JavaRDD<String> studentInfosRDD = sc.parallelize(studentInfoJSONs);
//		//sqlContext调用.json()读取studentInfosRDD中数据，生成一个DataFrame。RDD的格式是json的,就要调用.json()
//		DataFrame studentInfoDF = sqlContext.read().json(studentInfosRDD);
//
//		studentInfoDF.registerTempTable("student_infos");
//		//拼接sql语句
//		String sql = "select name, age from student_infos where name in (";
//		for(int i=0; i<goodStudentNames.size(); i++){
//			sql += "'" + goodStudentNames.get(i) + "'";   //select name, age from student_infos where name in (' ',' ');
//			if(i < goodStudentNames.size() - 1){
//				sql += ",";
//			}
//		}
//		sql += ")";
//
//		DataFrame goodStudentInfosDF = sqlContext.sql(sql);
//
//		// 然后将两份数据的DataFrame数据执行join的转化算子操作
//		 JavaPairRDD<String, Tuple2<Integer, Integer>>  goodStudentsRDD = studentScoresDF.javaRDD().mapToPair(
//
//				 new PairFunction<Row, String, Integer>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, Integer> call(Row row) throws Exception {
//						return new Tuple2<String,Integer>(String.valueOf(row.get(0)),
//								Integer.valueOf(String.valueOf(row.get(1))));
//					}
//		})
//		.join(goodStudentInfosDF.javaRDD().mapToPair(
//
//				new PairFunction<Row, String, Integer>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, Integer> call(Row row) throws Exception {
//						return new Tuple2<String, Integer>(String.valueOf(row.get(0)),
//								Integer.valueOf(String.valueOf(row.get(1))));
//					}
//		}));
//
//		//转换RDD的格式  (name,(score,age))
//		JavaRDD<Row> goodStudentsRowRDD = goodStudentsRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
//					throws Exception {
//				// Row(_._1, _._2._1, _._2._2)
//				return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
//			}
//		});
//
//		List<StructField> structFields = new ArrayList<StructField>();
//		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
//		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
//		StructType structType = DataTypes.createStructType(structFields);
//
//		DataFrame goodStudentDF = sqlContext.createDataFrame(goodStudentsRowRDD, structType);
//		goodStudentDF.write().format("json").mode(SaveMode.Overwrite).save("goodStudentJson_1");
//	}
//}
