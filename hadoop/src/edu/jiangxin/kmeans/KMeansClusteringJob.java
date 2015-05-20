package edu.jiangxin.kmeans;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class KMeansClusteringJob {

	public static void main(String[] args) throws Exception {

		int iteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");

		Path in = new Path("hdfs://localhost:9000/kmeans/data/cluster.txt");
		Path center = new Path(
				"hdfs://localhost:9000/kmeans/center/centers.txt");
		Path out = new Path("kmeans/depth_1");

		conf.set("centroid.path", center.toString());

		Job job = new Job(conf);
		job.setJobName("KMeans Clustering");

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansClusteringJob.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		long counter = job.getCounters()
				.findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		iteration++;
		long time = 0;
		// System.out.println(counter + " " + iteration);
		while (counter > 0 && iteration < 10) {
			conf = new Configuration();
			conf.set("centroid.path", center.toString());
			conf.set("num.iteration", iteration + "");
			job = new Job(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);

			in = new Path("hdfs://localhost:9000/kmeans/data/cluster.txt");
			out = new Path("kmeans/depth_" + iteration);

			FileInputFormat.addInputPath(job, in);
			if (fs.exists(out))
				fs.delete(out, true);

			FileOutputFormat.setOutputPath(job, out);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			long start = new Date().getTime();
			job.waitForCompletion(true);
			long end = new Date().getTime();
			System.out.println("Job took " + (end - start) + "milliseconds");
			time = time + (end - start);
			iteration++;
			counter = job.getCounters()
					.findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}

		System.out.println("Total time Taken :" + time);
	}
}
