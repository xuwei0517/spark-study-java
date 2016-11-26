package cn.spark.spark.sql;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
/**
 * 不能在本地运行
 * @author lenovo
 *
 */
public class DataFrameCreate {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("DataFrameCreate");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame df = sqlContext.read().json("hdfs://192.168.80.100:9000/students.json");
		df.show();
		
	}
}
