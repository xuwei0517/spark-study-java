package cn.spark.spark.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 加载parquet格式的数据
 * @author lenovo
 *
 */
public class ParquetLoadData {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("ParquetLoadData");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sc);
		//下面这两种读取parquet文件的方式都一样
		DataFrame df = sqlContext.read().parquet("hdfs://192.168.80.100:9000/users.parquet");
		//DataFrame df = sqlContext.read().format("parquet").load("hdfs://192.168.80.100:9000/users.parquet");
		df.registerTempTable("users");
		
		DataFrame sqldf = sqlContext.sql("select name from users");
		
		List<String> collect = sqldf.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return "name:"+row.getString(0);
			}
		}).collect();
		
		for (String str : collect) {
			System.out.println(str);
			
		}
	}
}
