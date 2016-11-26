package cn.spark.spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 基于排序的wordcount 倒序排序
 * @author lenovo
 *
 */
public class SortWordCount {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("sortwordcount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> linesRDD = sc.textFile("d:\\spark.txt");
		
		JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> wordCountRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairRDD<String, Integer> reduceByKeyRDD = wordCountRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//到此 获取到 单词和出现的次数
		//还需要进行排序，但是在排序之前需要把value和key的位置互换一下
		JavaPairRDD<Integer, String> countWordRDD = reduceByKeyRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t)
					throws Exception {
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		});
		
		JavaPairRDD<Integer, String> sortByKeyRDD = countWordRDD.sortByKey(false);
		
		JavaPairRDD<String, Integer> sortWordCount = sortByKeyRDD.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> t)
					throws Exception {
				return new Tuple2<String, Integer>(t._2, t._1);
			}
		});
		
		sortWordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"---"+t._2);
			}
		});
		
		sc.close();
	}
	
}
