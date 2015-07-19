package usth.iot.MovieLens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * optimize the combine table
 * step3中只是完成了矩阵相乘中的相乘，step4来完成相乘之后的累加(把字符串的某些相同字段提取出来，把他们的v2进行累加)
 */

public class Step4 {
	public static final String INPUT_PATH = "hdfs://192.168.56.100:9000/movie/step3/part*";
	public static final String OUT_PATH = "hdfs://192.168.56.100:9000/movie/step4";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, Step4.class.getSimpleName());
		job.setJarByClass(Step4.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, NewK2, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NewK2, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[:\t]");
			double v2 = Double.parseDouble(split[3]);
			context.write(new NewK2(Long.parseLong(split[2]), Long.parseLong(split[0])) , new DoubleWritable(v2));
		}
	}

	public static class MyReducer extends Reducer<NewK2, DoubleWritable, LongWritable, Text> {
		@Override
		protected void reduce(NewK2 k2,Iterable<DoubleWritable> v2s,
				Reducer<NewK2, DoubleWritable, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable num : v2s) {
				sum += num.get();
			}
			long k3 = k2.first;
			String v3 = k2.second+","+sum;
			context.write(new LongWritable(k3), new Text(v3));
		}
	}
	
	public static class NewK2 implements WritableComparable<NewK2>{
		long first;
		long second;
		
		public NewK2() {}
	
		public NewK2(long first,long second) {
			this.first = first;
			this.second = second;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}

		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}

		public int compareTo(NewK2 o) {
			int minus = (int)(this.first - o.first);
			if(minus!=0){
				return minus;
			}
			return (int)(this.second - o.second);
		}
		
	}
}
