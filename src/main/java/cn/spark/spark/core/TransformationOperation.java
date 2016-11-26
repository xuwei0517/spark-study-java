package cn.spark.spark.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import scala.Tuple2;


public class TransformationOperation {

	public static void main(String[] args) {
		//map();
		//filter();
		//flatmap();
		//groupbykey();
		//reduceBykey();
		//sortByKey();
		//join();
		cogroup();
	}
	
	private static void cogroup(){
		SparkConf sparkConf = new SparkConf().setAppName("cogroup").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Tuple2<Integer, String>> studentTupleList = Arrays.asList(
				new Tuple2<Integer, String>(1, "zs"),
				new Tuple2<Integer, String>(2, "ls"),
				new Tuple2<Integer, String>(3, "ww"),
				new Tuple2<Integer, String>(4, "zg"));
		
		List<Tuple2<Integer, Integer>> scoreTupleList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 80),
				new Tuple2<Integer, Integer>(1, 801),
				new Tuple2<Integer, Integer>(2, 59),
				new Tuple2<Integer, Integer>(2, 591),
				new Tuple2<Integer, Integer>(3, 65),
				new Tuple2<Integer, Integer>(3, 651),
				new Tuple2<Integer, Integer>(4, 100),
				new Tuple2<Integer, Integer>(4, 1001));
		
		JavaPairRDD<Integer, String> studentTupleRDD = sc.parallelizePairs(studentTupleList);
		JavaPairRDD<Integer, Integer> scoreTupleRDD = sc.parallelizePairs(scoreTupleList);
		
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = studentTupleRDD.cogroup(scoreTupleRDD);
		cogroupRDD.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
					throws Exception {
				System.out.println(t._1+"--{"+t._2._1+"--"+t._2._2+"}");
			}
		});
		
		
	}
	
	private static void join(){
		SparkConf sparkConf = new SparkConf().setAppName("join").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Tuple2<Integer, String>> studentTupleList = Arrays.asList(
				new Tuple2<Integer, String>(1, "zs"),
				new Tuple2<Integer, String>(2, "ls"),
				new Tuple2<Integer, String>(3, "ww"),
				new Tuple2<Integer, String>(4, "zg"));
		
		List<Tuple2<Integer, Integer>> scoreTupleList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 80),
				new Tuple2<Integer, Integer>(2, 59),
				new Tuple2<Integer, Integer>(3, 65),
				new Tuple2<Integer, Integer>(4, 100));
		
		JavaPairRDD<Integer, String> studentTupleRDD = sc.parallelizePairs(studentTupleList);
		JavaPairRDD<Integer, Integer> scoreTupleRDD = sc.parallelizePairs(scoreTupleList);
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = studentTupleRDD.join(scoreTupleRDD);
		joinRDD.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
					throws Exception {
				System.out.println(t._1+"--{"+t._2._1+"--"+t._2._2+"}");
			}
		});
		
		
	}
	
	private static void sortByKey(){
		SparkConf sparkConf = new SparkConf().setAppName("sortBykey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Tuple2<Integer, String>> tupleList = Arrays.asList(
				new Tuple2<Integer, String>(80, "zs"),
				new Tuple2<Integer, String>(59, "ls"),
				new Tuple2<Integer, String>(65, "ww"),
				new Tuple2<Integer, String>(100, "zg"));
		
		JavaPairRDD<Integer, String> tupleListRDD = sc.parallelizePairs(tupleList);
		JavaPairRDD<Integer, String> sortByKeyRDD = tupleListRDD.sortByKey(true);
		
		sortByKeyRDD.foreach(new VoidFunction<Tuple2<Integer,String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1+"----"+t._2);
			}
			
		});
		
		
	}
	
	private static void reduceBykey(){
		SparkConf sparkConf = new SparkConf().setAppName("groupByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Tuple2<String, Integer>> tupleList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));
		
		JavaPairRDD<String, Integer> tupleListRDD = sc.parallelizePairs(tupleList);
		
		JavaPairRDD<String, Integer> reduceByKeyRDD = tupleListRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"--"+t._2);
			}
		});
	}
	
	/**
	 * groupByKey
	 */
	private static void groupbykey(){
		SparkConf sparkConf = new SparkConf().setAppName("groupByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Tuple2<String, Integer>> tupleList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));
		
		JavaPairRDD<String, Integer> tupleListRDD = sc.parallelizePairs(tupleList);
		
		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = tupleListRDD.groupByKey();
		
		groupByKeyRDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				System.out.println("class："+t._1);
				Iterator<Integer> iterator = t._2.iterator();
				while(iterator.hasNext()){
					System.out.print(iterator.next()+" ");
				}
				System.out.println();
			}
		});
		
	}
	
	/**
	 * flatmap
	 */
	private static void flatmap(){
		SparkConf sparkConf = new SparkConf().setAppName("flatmap").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<String> lines = Arrays.asList("hello you","hello me");
		
		JavaRDD<String> linesRDD = sc.parallelize(lines);
		
		JavaRDD<String> flatMapRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		
		flatMapRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	/**
	 * filter
	 */
	private static void filter(){
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("filter").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numsRDD = sc.parallelize(numbers);
		JavaRDD<Integer> filterRDD = numsRDD.filter(new Function<Integer, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer v1) throws Exception {
				return v1 % 2==0;
			}
		});
		
		filterRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		sc.close();
	}
	
	/**
	 * map案例
	 */
	private static void map(){
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("map").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Integer> nums = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numsRDD = sc.parallelize(nums);
		
		JavaRDD<Integer> mapRDD = numsRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 *2 ;
			}
		});
		
		mapRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
		
	}
	
}
