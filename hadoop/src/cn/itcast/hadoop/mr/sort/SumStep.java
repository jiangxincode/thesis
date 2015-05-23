package cn.itcast.hadoop.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumStep {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(SumStep.class);

		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

	public static class SumMapper extends
			Mapper<LongWritable, Text, Text, InfoBean> {

		private InfoBean bean = new InfoBean();
		private Text k = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// split
			String line = value.toString();
			String[] fields = line.split("\t");
			// get useful field
			String account = fields[0];
			double income = Double.parseDouble(fields[1]);
			double expenses = Double.parseDouble(fields[2]);
			k.set(account);
			bean.set(account, income, expenses);
			context.write(k, bean);
		}
	}

	public static class SumReducer extends
			Reducer<Text, InfoBean, Text, InfoBean> {

		private InfoBean bean = new InfoBean();

		@Override
		protected void reduce(Text key, Iterable<InfoBean> v2s, Context context)
				throws IOException, InterruptedException {

			double in_sum = 0;
			double out_sum = 0;
			for (InfoBean bean : v2s) {
				in_sum += bean.getIncome();
				out_sum += bean.getExpenses();
			}
			bean.set("", in_sum, out_sum);
			context.write(key, bean);
		}

	}
}
