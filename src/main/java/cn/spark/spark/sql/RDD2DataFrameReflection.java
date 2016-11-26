package cn.spark.spark.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 通过反射的方式获取dataframe
 * @author lenovo
 *
 */
public class RDD2DataFrameReflection {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("RDD2DataFrameReflection");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> lines = sc.textFile("d:\\students.txt");
		//注意：student需要实现序列化，并且是public
		JavaRDD<Student> students = lines.map(new Function<String, Student>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Student call(String line) throws Exception {
				String[] splits = line.split(",");
				Student student = new Student(Integer.valueOf(splits[0].trim()),String.valueOf(splits[1]),Integer.valueOf(splits[2].trim()));
				return student;
			}
		});
		//把studentRDD转换为dataframe
		DataFrame df = sqlContext.createDataFrame(students, Student.class);
		//注册为一个临时表
		df.registerTempTable("students");
		
		//执行sql查询
		DataFrame sqldf = sqlContext.sql("select id,name,age from students where age >18");
		
		//将查询到的内容转换为RDD
		JavaRDD<Row> rowRDD = sqldf.javaRDD();
		
		JavaRDD<Student> studentRDD = rowRDD.map(new Function<Row, Student>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Student call(Row row) throws Exception {
				//在这可以根据角标获取列的内容，也可以根据列的字段名称获取值
				Student student = new Student(row.getInt(0), row.getAs("name").toString(), row.getInt(2));
				return student;
			}
		});
		
		List<Student> collect = studentRDD.collect();
		for (Student student : collect) {
			System.out.println(student);
		}
	}

}
