package cn.spark.spark.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class JdbcDataSource {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("JdbcDataSource");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);
		
		
		HashMap<String, String> options = new HashMap<String,String>();
		options.put("url", "jdbc:mysql://192.168.80.1:3306/aaa");
		options.put("dbtable", "student_infos");
		options.put("user", "crxy");
		options.put("password", "crxy");
		DataFrame studentinfosDF = sqlContext.read().format("jdbc").options(options).load();
		
		options.put("dbtable", "student_scores");
		DataFrame studentscoresDF = sqlContext.read().format("jdbc").options(options).load();
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = studentinfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(String.valueOf(row.get(0)),Integer.valueOf(String.valueOf(row.get(1))));
			}
		}).join(studentscoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(String.valueOf(row.get(0)),Integer.valueOf(String.valueOf(row.get(1))));
			}
		}));
		
		JavaRDD<Row> rowRDD = joinRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
					throws Exception {
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
		});
		
		JavaRDD<Row> goodStudentRDD = rowRDD.filter(new Function<Row, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Row row) throws Exception {
				
				if(row.getInt(2)>=80){
					return true;
				}else{
					return false;
				}
			}
		});
		List<Row> collect = goodStudentRDD.collect();
		for (Row row : collect) {
			System.out.println(row);
		}
		goodStudentRDD.foreach(new VoidFunction<Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Row row) throws Exception {
				Class.forName("com.mysql.jdbc.Driver");
				String sql = "insert into good_student_infos values(" 
						+ "'" + String.valueOf(row.getString(0)) + "',"
						+ Integer.valueOf(String.valueOf(row.get(1))) + ","
						+ Integer.valueOf(String.valueOf(row.get(2))) + ")";
				Connection connection = null;
				Statement state = null;
				try {
					connection = DriverManager.getConnection("jdbc:mysql://192.168.80.1:3306/aaa", "crxy", "crxy");
					state = connection.createStatement();
					state.executeUpdate(sql);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					if(state!=null){
						state.close();
					}
					if(connection!=null){
						connection.close();
					}
				}
			}
		});
	}

}
