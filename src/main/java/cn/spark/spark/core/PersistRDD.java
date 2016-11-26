package cn.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * RDD持久化
 * @author lenovo
 *
 */
public class PersistRDD {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("persist").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		/**
		 * 调用cache或者persist进行持久化
		 * 其实cache就是persist的一种默认情况，只持久化在内存中
		 * 
		 */
		JavaRDD<String> textFileRDD = sc.textFile("d:\\goodsinfo.log").cache();
		long start_time = System.currentTimeMillis();
		long count = textFileRDD.count();
		System.out.println("count1："+count);
		long end_time = System.currentTimeMillis();
		System.out.println("cost time1："+(end_time-start_time));
		
		
		start_time = System.currentTimeMillis();
		count = textFileRDD.count();
		System.out.println("count2："+count);
		end_time = System.currentTimeMillis();
		System.out.println("cost time2："+(end_time-start_time));
		
		sc.close();
	}
	

}
