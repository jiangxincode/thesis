package edu.jiangxin.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final List<String> centers = new ArrayList<String>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);

		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(centroids)));
		String line = null;
		while ((line = br.readLine()) != null) {
			centers.add(line);
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		int dimension = value.toString().split(",").length;
		for (String c : centers) {
			double dist = dist(c, value.toString(), dimension);
			if (null == nearest) {
				nearest = c;
				nearestDistance = dist;
			} else {
				if (nearestDistance > dist) {
					nearest = c;
					nearestDistance = dist;
				}
			}
		}
		Text word = new Text();
		word.set(nearest);
		context.write(word, value);
	}

	private static double dist(String point, String center, int dimension) {
		double sum = 0.0;
		for(int i=0;i<dimension;i++) {
			double point_i = Double.parseDouble(point.split(",")[i]);
			double center_i = Double.parseDouble(center.split(",")[i]);
			sum += Math.pow((point_i - center_i), 2);
		}
		return Math.sqrt(sum);
	}
}
