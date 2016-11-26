package cn.spark.spark.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class DailyTop3Keyword {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("DailyTop3Keyword");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		HiveContext hiveContext = new HiveContext(sc.sc());
		
		//查询条件
		HashMap<String, List<String>> queryParamMap = new HashMap<String,List<String>>();
		queryParamMap.put("city", Arrays.asList("beijing"));  //城市 
		queryParamMap.put("platform", Arrays.asList("android"));  //平台
		queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));  //版本
		
		//需要对查询条件这个map进行优化，设置为broadcast广播变量，这样的话每个节点上面，只会拷贝一份数据
		final Broadcast<HashMap<String, List<String>>> queryParamMapBroadcast = sc.broadcast(queryParamMap);
		
		JavaRDD<String> lines = sc.textFile("hdfs://192.168.80.100:9000/spark-study/keyword.txt");
		
		//对数据进行过滤
		JavaRDD<String> filterRDD = lines.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String str) throws Exception {
				String[] splits = str.split("\t");
				String city = splits[3];
				String platform = splits[4];
				String version = splits[5];
				
				//从全局变量中获取查询条件
				HashMap<String, List<String>> value = queryParamMapBroadcast.getValue();
				List<String> citys = value.get("city");
				if(citys.size()>0 && !citys.contains(city)){
					return false;
				}
				
				List<String> platforms = value.get("platform");
				if(platforms.size()>0 && !platforms.contains(platform)){
					return false;
				}
				
				List<String> versions = value.get("version");
				if(versions.size()>0 && !versions.contains(version)){
					return false;
				}
				
				return true;
			}
		});
		
		//把过滤之后的数据 映射为(日期_搜索词,用户)的格式
		JavaPairRDD<String, String> dateKeywordUserRDD = filterRDD.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				String[] splits = line.split("\t");
				String date = splits[0];
				String user = splits[1];
				String keyword = splits[2];
				return new Tuple2<String, String>(date+"_"+keyword, user);
			}
		});
		
		//根据日期_搜索词进行分组，获取对应的所有用户
		JavaPairRDD<String, Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();
		
		//执行去重操作，获取uv
		JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, Long>() {

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers)
					throws Exception {
				String dateKeyword =  dateKeywordUsers._1;
				Iterator<String> users = dateKeywordUsers._2.iterator();
				//对用户进行去重，并统计去重后的数量
				ArrayList<String> distinctUsers = new ArrayList<String>();
				
				while(users.hasNext()){
					String user = users.next();
					if(!distinctUsers.contains(user)){
						distinctUsers.add(user);
					}
				}
				
				return new Tuple2<String, Long>(dateKeyword, (long)distinctUsers.size());
			}
		});
		//把搜索词的uv的数据，转换成dataframe,为了后面进行sql查询
		JavaRDD<Row> dateKeywordUvDF = dateKeywordUvRDD.map(new Function<Tuple2<String,Long>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
				String date = dateKeywordUv._1.split("_")[0];
				String keyword = dateKeywordUv._1.split("_")[1];
				long uv = dateKeywordUv._2;
				return RowFactory.create(date,keyword,uv);
			}
		});
		//组装元数据
		List<StructField> structFields = Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("keyword", DataTypes.StringType, true),
				DataTypes.createStructField("uv", DataTypes.LongType, true));
		
		StructType StructTypes = DataTypes.createStructType(structFields);
		
		DataFrame dataKeyWordUvDF = hiveContext.createDataFrame(dateKeywordUvDF, StructTypes);
		
		dataKeyWordUvDF.registerTempTable("daily_keyword_uv");
		
		//取前三
		DataFrame dailyTop3KeywordDF = hiveContext.sql(""
				+ "select date,keyword,uv "
				+ "from ("
					+ "	select date,keyword,uv,row_number() over (partition by date order by uv) rank "
					+ "from daily_keyword_uv"
				+ ") tmp"
				+ " where rank <=3");
		
		dailyTop3KeywordDF.saveAsTable("daily_top3_keyword_uv");
		
		sc.close();
		
		
		
	}
}
