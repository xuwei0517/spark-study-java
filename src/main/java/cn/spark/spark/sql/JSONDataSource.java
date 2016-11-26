package cn.spark.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class JSONDataSource {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("ParquetLoadData");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame studentScoreDF = sqlContext.read().json("hdfs://192.168.80.100:9000/students.json");
		studentScoreDF.registerTempTable("student_scores");
		
		JavaRDD<Row> goodStudentScoreRDD = sqlContext.sql("select name,score from student_scores where score>=60").javaRDD();
		
		//获取满足条件的学员的姓名，方便后期sql拼接。
		List<String> studentNames = goodStudentScoreRDD.map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Row row) throws Exception {
				return row.getString(0);
			}
		}).collect();
		
		
		//创建包含学生基本信息的RDD
		ArrayList<String> studentInfosJson = new ArrayList<String>();
		studentInfosJson.add("{\"name\":\"zs\",\"age\":18}");
		studentInfosJson.add("{\"name\":\"ls\",\"age\":28}");
		studentInfosJson.add("{\"name\":\"ww\",\"age\":38}");
		
		JavaRDD<String> studentInfosJsonRDD = sc.parallelize(studentInfosJson);
		DataFrame studentInfosJsonDF = sqlContext.read().json(studentInfosJsonRDD);
		studentInfosJsonDF.registerTempTable("student_infos");
		
		String sqlText = "select name,age from student_infos where name in (";
		for (int i = 0; i < studentNames.size(); i++) {
			if(i==0){
				sqlText+="'"+studentNames.get(i)+"'";
			}else{
				sqlText+=",'"+studentNames.get(i)+"'";
			}
		}
		sqlText+=")";
		
		JavaRDD<Row> goodStudentInfosJsonRDD = sqlContext.sql(sqlText).javaRDD();
		//对两个RDD进行join操作
		JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = goodStudentScoreRDD.mapToPair(new PairFunction<Row, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				//注意：默认情况下 sparksql会把数字类型的封装为long类型
				return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
			}
		}).join(goodStudentInfosJsonRDD.mapToPair(new PairFunction<Row, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
			}
		}));
		
		//最终要把RDD中的内容写到hdfs的json文件中，所以还要把RDD转换为dataframe
		
		JavaRDD<Row> rowsMapRDD = joinRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
					throws Exception {
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
		});
		ArrayList<StructField> structFieldList = new ArrayList<StructField>();
		structFieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFieldList.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		structFieldList.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		
		StructType structType = DataTypes.createStructType(structFieldList);
		
		DataFrame goodStudentsDF = sqlContext.createDataFrame(rowsMapRDD, structType);
		
		goodStudentsDF.write().json("hdfs://192.168.80.100:9000/good-students");
	}

}
