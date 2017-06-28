package Moudle.Spark1.sxt.cong.sparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 2016/8/31.
 */
public class DataFrame_test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame dt = sqlContext.read().json("students.json");
        dt.registerTempTable("student");

        DataFrame dt1 = sqlContext.sql("select name from student where score >90");


        dt1.show();
    }

}
