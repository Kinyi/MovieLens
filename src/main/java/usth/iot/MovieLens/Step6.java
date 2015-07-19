package usth.iot.MovieLens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * sorted by the score 
 */

public class Step6 {
	public static final String INPUT_PATH = "hdfs://192.168.56.100:9000/movie/step5/part*";
	public static final String OUT_PATH = "hdfs://192.168.56.100:9000/movie/step6";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, Step6.class.getSimpleName());
		job.setJarByClass(Step6.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable>{
		LongWritable v2 = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NewK2, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");
			v2.set(Long.parseLong(split[1]));
			context.write(new NewK2(Long.parseLong(split[0]), Double.parseDouble(split[2])), v2);
		}
	}
	
	public static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, Text>{
		LongWritable k3 = new LongWritable();
		Text v3 = new Text();
		
		@Override
		protected void reduce(NewK2 k2, java.lang.Iterable<LongWritable> v2s,
				org.apache.hadoop.mapreduce.Reducer<NewK2,LongWritable,LongWritable,Text>.Context context)
				throws IOException ,InterruptedException {
			for (LongWritable movie : v2s) {
				k3.set(k2.first);
				v3.set(movie+","+k2.second);
				context.write(k3, v3);
			}
		};
	}
	
	public static class NewK2 implements WritableComparable<NewK2>{
		long first;
		double second;
		
		public NewK2(){}
		
		public NewK2(long first, double second){
			this.first = first;
			this.second = second;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeDouble(second);
		}

		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readDouble();
		}

		public int compareTo(NewK2 o) {
			int minus = (int)(this.first - o.first);
			if(minus != 0){
				return minus;
			}
			return (int)(o.second - this.second);
		}
		
	}
}
