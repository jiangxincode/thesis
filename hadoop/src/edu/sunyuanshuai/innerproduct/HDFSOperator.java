package edu.sunyuanshuai.innerproduct;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSOperator {
	public static boolean deleteDir(String dir) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		boolean result = fs.delete(new Path(dir), true);
		fs.close();
		return result;
	}
}
