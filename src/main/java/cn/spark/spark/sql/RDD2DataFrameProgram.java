package cn.spark.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 以编程方式动态指定元数据 将RDD转换为DateFrame
 * @author lenovo
 *
 */
public class RDD2DataFrameProgram {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("RDD2DataFrameReflection");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> lines = sc.textFile("d:\\students.txt");
		//要把这个RDD转换为RDD<ROW>的形式
		JavaRDD<Row> rowRDD = lines.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Row call(String line) throws Exception {
				String[] splits = line.split(",");
				//注意：在创建row的时候要注意不同字段的值的类型
				return RowFactory.create(Integer.valueOf(splits[0]),splits[1],Integer.parseInt(splits[2]));
			}
		});
		
		/**
		 * 动态构造元数据
		 * 下面list中的字段，是可以根据数据库中的内容或者配置文件进行动态添加的
		 */
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		
		StructType structType = DataTypes.createStructType(fields);
		
		DataFrame df = sqlContext.createDataFrame(rowRDD, structType);
		
		df.registerTempTable("students");
		
		DataFrame sqldf = sqlContext.sql("select id,name,age from students where age > 18");
		
		List<Row> collect = sqldf.javaRDD().collect();
		for (Row row : collect) {
			System.out.println(row.toString());
		}
	}
}
