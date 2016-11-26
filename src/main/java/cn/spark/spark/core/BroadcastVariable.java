package cn.spark.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 共享变量 只读
 * @author lenovo
 *
 */
public class BroadcastVariable {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("broadcast").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		//默认情况下 map方法产生的所有task中都会有一份factor变量，这样会浪费内存，并且也会增加数据传输时间
		//为了减少内存占用，提高性能，可以使用共享变量，在节点上只保存一个，所有task共享这一份  使用sc的broadcast方法
		//但是注意，broadcast方法创建的共享变量是只读的
		int factor = 2;
		//创建共享变量，使用的时候调用 其value方法即可
		final Broadcast<Integer> broadcastFactor = sc.broadcast(factor);
		
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);
		
		JavaRDD<Integer> mapRDD = numberListRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 * broadcastFactor.value();
			}
		});
		
		mapRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		sc.close();
	}

}
