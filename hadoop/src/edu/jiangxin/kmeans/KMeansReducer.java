package edu.jiangxin.kmeans;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// calculate a new clustercenter for these vertices
public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

	public static enum Counter {
		CONVERGED
	}

	private final List<String> centers = new ArrayList<String>();
	int cluster = 0;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int dimension = values.toString().split(",").length;
		Double[] newCentroid_i = new Double[dimension];
		int count = 0;
		for (Text value : values) {
			for(int i=0;i<dimension;i++) {
				newCentroid_i[i] += Double.parseDouble(value.toString().split(",")[i]);
			}
			count++;
		}
		cluster++;
		for(int i=0;i<dimension;i++) {
			newCentroid_i[i] = newCentroid_i[i] / count;
		}
		String newCentroid = "";
		for(int i=0;i<dimension-1;i++) {
			newCentroid += newCentroid_i[i] + ",";
		}
		newCentroid += newCentroid_i[dimension-1];
		centers.add(newCentroid);
		if (!checkConvergence(key.toString(), newCentroid, dimension))
			context.getCounter(Counter.CONVERGED).increment(1);
	}

	private static boolean checkConvergence(String point, String center, int dimension) {

		double sum = 0.0;
		for(int i=0;i<dimension;i++) {
			double point_i = Double.parseDouble(point.split(",")[i]);
			double center_i = Double.parseDouble(center.split(",")[i]);
			sum += Math.pow((point_i - center_i), 2);
		}
		return Math.sqrt(sum) < 0.2;
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(centroids, true);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(centroids)));
		for (String center : centers)
			br.write(center + "\n");
		br.flush();
		br.close();
	}
}
