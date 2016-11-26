package cn.spark.spark.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class ActionOperation {
	public static void main(String[] args) {
		//reduce();
		//collect();
		//count();
		//take();
		//saveAsTextFile();
		countBykey();
	}
	
	private static void countBykey(){
		SparkConf sparkConf = new SparkConf().setAppName("countBykey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Tuple2<String, String>> tupleList = Arrays.asList(
				new Tuple2<String, String>("class1", "zs"),
				new Tuple2<String, String>("class2", "ls"),
				new Tuple2<String, String>("class1", "hehe"),
				new Tuple2<String, String>("class2", "haha"),
				new Tuple2<String, String>("class2", "ww"));
		
		JavaPairRDD<String, String> tupleListRDD = sc.parallelizePairs(tupleList);
		Map<String, Object> countByKey = tupleListRDD.countByKey();
		for (Entry<String, Object> entry : countByKey.entrySet()) {
			System.out.println(entry.getKey()+"--"+entry.getValue());
		}
		sc.close();
	}
	
	private static void saveAsTextFile(){
		SparkConf sparkConf = new SparkConf().setAppName("saveAsTextFile");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);
		JavaRDD<Integer> mapRDD = numberListRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		//结果会保存到这个目录下面
		mapRDD.saveAsTextFile("hdfs://hadoop100:9000/result");
		sc.close();
	}
	
	/**
	 * take和collect类型，都是从远程集群获取RDD数据到本地
	 * collect是获取所有的RDD数据，而take是获取指定数据
	 */
	private static void take(){
		SparkConf sparkConf = new SparkConf().setAppName("collect").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);
		JavaRDD<Integer> mapRDD = numberListRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		
		List<Integer> top3 = mapRDD.take(3);
		for (Integer integer : top3) {
			System.out.print(integer+" ");
		}
		sc.close();
	}
	
	private static void count(){
		SparkConf sparkConf = new SparkConf().setAppName("collect").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);
		JavaRDD<Integer> mapRDD = numberListRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		
		long count = mapRDD.count();
		System.out.println("总数："+count);
		sc.close();
	}
	
	
	/**
	 * 此功能可以实现类foreach action类似的功能，可以吧远程集群中的数据集拉取到本地进行处理
	 * 但是注意，如果数据量比较大的话可能会出现oom内存溢出问题。
	 */
	private static void collect(){
		SparkConf sparkConf = new SparkConf().setAppName("collect").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);
		JavaRDD<Integer> mapRDD = numberListRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		
		List<Integer> collect = mapRDD.collect();
		for (Integer integer : collect) {
			System.out.print(integer+" ");
		}
		
		sc.close();
	}
	
	private static void reduce(){
		SparkConf sparkConf = new SparkConf().setAppName("reduce").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		//对1-10的数据进行累加求和
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);
		Integer reduceRDD = numberListRDD.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		System.out.println("和为："+reduceRDD);
		sc.close();
	}
}
