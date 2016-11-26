package cn.spark.spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class LineCountLocal {
	
	public static void main(String[] args) {
		
		//创建sparkconf对象，设置spark的配置信息
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName(LineCountLocal.class.getSimpleName());
		sparkConf.setMaster("local");
		
		//创建javaspark上下文对象
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> lines = sc.textFile("d:\\hello.txt");
		
		
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				return new Tuple2<String, Integer>(line, 1);
			}
		});
		
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"----"+t._2);
			}
		});
		
		sc.close();
		
	}

}
