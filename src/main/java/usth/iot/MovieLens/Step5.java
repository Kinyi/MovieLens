package usth.iot.MovieLens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * exclude the movie user had seen
 */

public class Step5 {
	public static final String INPUT_PATH1 = "hdfs://192.168.56.100:9000/movie/step1/part*";
	public static final String INPUT_PATH2 = "hdfs://192.168.56.100:9000/movie/step4/part*";
	public static final String OUT_PATH = "hdfs://192.168.56.100:9000/movie/step5";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH1), conf);
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, Step5.class.getSimpleName());
		job.setJarByClass(Step5.class);
		MultipleInputs.addInputPath(job, new Path(INPUT_PATH1), TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, new Path(INPUT_PATH2), TextInputFormat.class, MyMapper2.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper1 extends Mapper<LongWritable, Text, NewK2, DoubleWritable> {
		@Override
		protected void map(LongWritable key,Text value,
				Mapper<LongWritable, Text, NewK2, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");
			for (int i = 1; i < split.length; i++) {
				String[] split2 = split[i].split(":");
				double v2 = Double.parseDouble(split2[1]);
				context.write(new NewK2(Long.parseLong(split[0]), Long.parseLong(split2[0])), new DoubleWritable(v2));
			}
		}
	}

	public static class MyMapper2 extends Mapper<LongWritable, Text, NewK2, DoubleWritable> {
		@Override
		protected void map(LongWritable key,Text value,
				Mapper<LongWritable, Text, NewK2, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");
			double v2 = Double.parseDouble(split[2]);
			context.write(new NewK2(Long.parseLong(split[0]), Long.parseLong(split[1])), new DoubleWritable(v2));
		}
	}

	public static class MyReducer extends Reducer<NewK2, DoubleWritable, LongWritable, Text> {
		@Override
		protected void reduce(NewK2 k2,Iterable<DoubleWritable> v2s,
				Reducer<NewK2, DoubleWritable, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			ArrayList<Double> list = new ArrayList<Double>();
			for (DoubleWritable time : v2s) {
				list.add(time.get());
			}
			if (list.size() == 1) {
				String v3 = k2.second + "," + list.get(0);
				context.write(new LongWritable(k2.first), new Text(v3));
			}
		}
	}

	public static class NewK2 implements WritableComparable<NewK2> {
		long first;
		long second;

		public NewK2() {
		}

		public NewK2(long first, long second) {
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
			int minus = (int) (this.first - o.first);
			if (minus != 0) {
				return minus;
			}
			return (int) (this.second - o.second);
		}
	}
}
