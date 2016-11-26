package cn.spark.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * parquet数据源之自动推断分区
 * 在hdfs上创建目录
 *  hdfs://192.168.80.100:9000/users/gender=male/country=US
 *  在把users.parquet数据上传到这个目录下
 * @author lenovo
 *
 */
public class ParquetPartitionDiscovery {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");conf.setAppName("RDD2DataFrameReflection");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.read()
				.parquet("hdfs://192.168.80.100:9000/users/gender=male/country=US/users.parquet");
		df.printSchema();
		/** +------+--------------+----------------+------+-------+
			|  name|favorite_color|favorite_numbers|gender|country|
			+------+--------------+----------------+------+-------+
			|Alyssa|          null|  [3, 9, 15, 20]|  male|     US|
			|   Ben|           red|              []|  male|     US|
			+------+--------------+----------------+------+-------+
		 */
		df.show();
	}
}
