package Moudle.Spark1.sxt.sparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2016/8/31.
 */
public class JSONDataSource_JTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame studentInfoDT = sqlContext.read().json("students.json");
        studentInfoDT.registerTempTable("studentInfo");

        DataFrame goodstudentInfoDT = sqlContext.sql("select name , score from studentInfo where score > 80");

        List<String> goodstudentsName = goodstudentInfoDT.javaRDD().map(new Function<Row, String>() {

            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();


        JavaRDD<Row> goodstudentInfoRDD = goodstudentInfoDT.javaRDD();

        List<String> studentageInfo = new ArrayList<String>();
        studentageInfo.add("{\"name\":\"Cpeixin\", \"age\":22}");
        studentageInfo.add("{\"name\":\"Nazha\", \"age\":23}");
        studentageInfo.add("{\"name\":\"Hyuting\", \"age\":24}");
        studentageInfo.add("\"name\":\"Zchuang\", \"age\":25");

        JavaRDD<String> studentageInfoRDD = sc.parallelize(studentageInfo);

        //RDD转化成DataFrame

        DataFrame studentageInfoDT  = sqlContext.read().json(studentageInfoRDD);

        studentageInfoDT.registerTempTable("studentageInfo");

        String sql = "select name, age from studentageInfo where name in (";
        for(int i=0; i<goodstudentsName.size(); i++){
            sql += "'" + goodstudentsName.get(i) + "'";   //select name, age from student_infos where name in (' ',' ');
            if(i < goodstudentsName.size() - 1){
                sql += ",";
            }
        }
        sql += ")";

        DataFrame goodstudentageInfoDT = sqlContext.sql(sql);

        JavaPairRDD<String, Tuple2<Integer, Integer>> goodstudentRDD = goodstudentInfoRDD.mapToPair(new PairFunction<Row, String, Integer>() {


            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(String.valueOf(row.get(0)),
                        Integer.valueOf(String.valueOf(row.get(1))));

            }
        }).join(goodstudentageInfoDT.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(String.valueOf(row.get(0)),
                        Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));

        //转换RDD的格式  (name,(score,age))
        JavaRDD<Row> goodStudentsRowRDD = goodstudentRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
                    throws Exception {
                // Row(_._1, _._2._1, _._2._2)
                return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame goodStudentDF = sqlContext.createDataFrame(goodStudentsRowRDD, structType);
        goodStudentDF.write().format("json").mode(SaveMode.Overwrite).save("goodStudentJson");
    }

}
