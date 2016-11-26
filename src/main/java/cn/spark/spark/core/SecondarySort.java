package cn.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 二次排序
 * 1：实现自定义的key，要实现ordered接口和serializable接口，在key中实现自己对多个列的排序算法
 * 2：将包含文本的RDD，映射成key为自定义key，value为原始文本内的的pairRDD
 * 3：使用sortbykey算子按照自定义key进行排序
 * 4：再次映射，剔除自定义的key，只保留文本行
 * @author lenovo
 *
 */
public class SecondarySort {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("secondarysort").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> lines = sc.textFile("d://sort.txt");
		JavaPairRDD<SecondarySortKey, String> pairRDD = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<SecondarySortKey, String> call(String line)
					throws Exception {
				String[] splits = line.split(" ");
				SecondarySortKey secondarySortKey = new SecondarySortKey(Integer.valueOf(splits[0]), Integer.valueOf(splits[1]));
				return new Tuple2<SecondarySortKey, String>(secondarySortKey, line);
			}
		});
		
		
		JavaPairRDD<SecondarySortKey, String> sortedPairRDD = pairRDD.sortByKey();
		JavaRDD<String> sortedRDD = sortedPairRDD.map(new Function<Tuple2<SecondarySortKey,String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<SecondarySortKey, String> v1)
					throws Exception {
				return v1._2;
			}
		});
		
		sortedRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.close();
	}

}
