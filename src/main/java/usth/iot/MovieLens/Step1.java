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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*
 * generate the user table
 */

public class Step1 {
	public static final String INPUT_PATH = "hdfs://192.168.56.100:9000/movie/ratings";
	public static final String OUT_PATH = "hdfs://192.168.56.100:9000/movie/step1";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, Step1.class.getSimpleName());
		job.setJarByClass(Step1.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			//把每一条记录按'::'进行分割
			String[] split = value.toString().split("::");
			//取用户ID作为k2
			long k2 = Long.parseLong(split[0]);
			//把电影ID和用户评分以':'相隔作为v2
			String tmp = split[1]+":"+split[2];
			context.write(new LongWritable(k2), new Text(tmp));
		}
	}
	
	public static class MyReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,
				Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String v3 = "";
			for (Text v2 : v2s) {
				//把相同用户所看的电影及评分用','隔开，并作为v3输出
				v3 = v3+","+v2.toString();
			}
			context.write(k2, new Text(v3.substring(1)));
		}
	}

}
