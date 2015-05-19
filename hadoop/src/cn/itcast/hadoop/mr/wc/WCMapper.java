package cn.itcast.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//接收一行数据
		String line = value.toString();
		//分割
		String[] words = line.split(" ");
		//迭代
		for(String w : words){
			//发送
			context.write(new Text(w), new LongWritable(1));
		}
	}

	
}
