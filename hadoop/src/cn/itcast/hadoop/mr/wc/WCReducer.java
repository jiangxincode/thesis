package cn.itcast.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> v2s, Context context)
			throws IOException, InterruptedException {
		long sum = 0; // 定义一行计数器
		for (LongWritable lw : v2s) {
			sum += lw.get(); // 求和
		}
		context.write(key, new LongWritable(sum)); // 输出
	}
}
