package cn.spark.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class DataFrameOperation {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("DataFrameCreate");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sc);
		//创建出来的dataframe可以理解为一张表
		DataFrame df = sqlContext.read().json("hdfs://192.168.80.100:9000/students.json");
		//打印dataframe中的所有数据 类似(select * from ...)
		df.show();
		//打印元数据信息 其实就是打印每个字段的属性信息
		/**
		 * root
		 |-- age: long (nullable = true)
		 |-- id: long (nullable = true)
		 |-- name: string (nullable = true)
		 */
		df.printSchema();
		//查询某列所有的数据
		df.select("name").show();
		//查询某几列所有的数据，并对age列进行计算
		df.select(df.col("name"),df.col("age").plus(1)).show();
		//根据某一类的值进行过滤
		df.filter(df.col("age").gt(18)).show();
		//根据某一列进行分组，然后进行聚合
		df.groupBy("age").count().show();
		
		
	}

}
