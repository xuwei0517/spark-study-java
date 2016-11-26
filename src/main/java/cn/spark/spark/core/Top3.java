package cn.spark.spark.core;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Top3 {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("top3").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile("d://top3.txt");
		JavaPairRDD<Integer, String> pairRDD = lines.mapToPair(new PairFunction<String, Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				return new Tuple2<Integer, String>(Integer.valueOf(t),t);
			}
		});
		
		JavaPairRDD<Integer, String> sortByKeyRDD = pairRDD.sortByKey(false);
		JavaRDD<String> sortedRDD = sortByKeyRDD.map(new Function<Tuple2<Integer,String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, String> v1) throws Exception {
				return v1._2;
			}
		});
		List<String> take = sortedRDD.take(3);
		for (String num : take) {
			System.out.println(num);
		}
		
		sc.close();
	}

}
