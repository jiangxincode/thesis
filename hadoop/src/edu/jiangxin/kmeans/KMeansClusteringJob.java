package edu.jiangxin.kmeans;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansClusteringJob {

	public static void main(String[] args) throws Exception {
		int iteration = 1;
		long counter = 1;
		long time = 0;
		Path in = new Path(args[0]);
		Path center = new Path(args[1]);
		Path out = new Path("kmeans/result");
		
		Configuration conf = new Configuration();
		conf.set("centroid.path", center.toString());
		conf.set("num.iteration", iteration + "");
		conf.set("num.dimension", args[2]);
		conf.set("num.precision", args[3]);
		
		while (counter > 0) {
			Job job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering " + iteration);
			
			job.setMapperClass(KMeansMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setJarByClass(KMeansMapper.class);
			
			job.setNumReduceTasks(Integer.parseInt(args[4]));
			
			FileInputFormat.addInputPath(job, in);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(out))
				fs.delete(out, true);
			FileOutputFormat.setOutputPath(job, out);
			
			long start = new Date().getTime();
			job.waitForCompletion(false);
			long end = new Date().getTime();
			time = time + (end - start);
			iteration++;
			counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}

		System.out.println("Total time Taken :" + time + "milliseconds");
	}
}
