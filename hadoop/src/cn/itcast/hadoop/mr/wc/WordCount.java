package cn.itcast.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// conf.set("mapreduce.input.fileinputformat.split.maxsize", "" +
		// 64*1024*1024);
		// conf.set("mapreduce.input.fileinputformat.split.minsize", "" +
		// 256*1024*1024);

		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class); // 设置jar

		// 设置Mapper相关的属性
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0])); //words.txt

		// 设置Reducer相关属性
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setCombinerClass(WCReducer.class);
		job.waitForCompletion(true); // 提交任务
	}
}
