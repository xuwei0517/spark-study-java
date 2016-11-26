package cn.spark.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 并行化集合创建RDD
 * 案例1：实现1~10累加求和
 * @author lenovo
 *
 */
public class ParallelizeCollection {
	public static void main(String[] args) {
		
		//获取javasparkcontext
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local");
		sparkConf.setAppName("ParallelizeCollection");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		//组装加载集合
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numsRDD = sc.parallelize(numbers);
		
		Integer sum = numsRDD.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		System.out.println("1~10的和为："+sum);
		
		sc.close();
		
	}

}
