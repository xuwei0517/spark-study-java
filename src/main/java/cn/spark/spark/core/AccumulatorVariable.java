package cn.spark.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 共享变量 可写 求和计算
 * @author lenovo
 *
 */
public class AccumulatorVariable {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("broadcast").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		final Accumulator<Integer> accumulator = sc.accumulator(0);
		
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);
		
		
		numberListRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				accumulator.add(t);
			}
		});
		
		System.out.println(accumulator);
		sc.close();
	}
}
