package usth.iot.MovieLens;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 *generate the cooccurence table 
 */

public class Step2 {
	public static final String INPUT_PATH = "hdfs://192.168.56.100:9000/movie/step1/part*";
	public static final String OUT_PATH = "hdfs://192.168.56.100:9000/movie/step2";
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, Step2.class.getSimpleName());
		job.setJarByClass(Step2.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//把用户表中的每一条数据按照特定分隔符进行分割
			String[] split = value.toString().split("[\t,]");
			//使用两个for循环，找到每个用户所看电影，单独出现计数及两两一组计数
			for (int i = 1; i < split.length; i++) {
				String[] split2 = split[i].split(":");
				for (int j = 1; j < split.length; j++) {
					String[] split3 = split[j].split(":");
					String v2 = split2[0]+":"+split3[0];
					context.write(new Text(v2), new LongWritable(1L));
				}
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		//统计同现矩阵的次数
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			Long count = 0L;
			for (LongWritable num : v2s) {
				count += num.get();
			}
			context.write(k2, new LongWritable(count));
		}
	}

}
