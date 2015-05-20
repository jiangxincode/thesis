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

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int dimension = 2;

		
		Double[] newCentroid_i = new Double[dimension];
		for(int i=0;i<dimension;i++) {
			newCentroid_i[i] = new Double(0.0);
		}
		int count = 0;
		for(Text value : values) {
			for(int i=0;i<dimension;i++) {
				newCentroid_i[i] += Double.parseDouble(value.toString().split(",")[i]);
			}
			count++;
		}
		System.out.println(count);
		System.out.println();
		System.out.println();
		
		for(int i=0;i<dimension;i++) {
			newCentroid_i[i] = newCentroid_i[i] / count;
		}
		String newCentroid = "";
		for(int i=0;i<dimension-1;i++) {
			newCentroid += newCentroid_i[i].toString() + ",";
		}
		newCentroid += newCentroid_i[dimension-1].toString();
		centers.add(newCentroid);
		System.out.println(newCentroid);
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
