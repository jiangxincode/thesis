package cn.itcast.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {
	
	private FileSystem fs = null;
	@Before
	public void init() throws IOException, URISyntaxException, InterruptedException{
		fs = FileSystem.get(new URI("hdfs://itcast01:9000"), new Configuration(),"root");
	}
	@Test
	public void  testDel() throws IllegalArgumentException, IOException{
		boolean flag = fs.delete(new Path("/words.txt"), true);
		System.out.println(flag);
	}
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException{
		boolean flag = fs.mkdirs(new Path("/itcast88888888"));
		System.out.println(flag);
	}
	@Test
	public void testUpload() throws IllegalArgumentException, IOException{
		FSDataOutputStream out = fs.create(new Path("/words.txt"));
		
		FileInputStream in = new FileInputStream(new File("c:/w.txt"));
		
		IOUtils.copyBytes(in, out, 2048, true);
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		
		FileSystem fs = FileSystem.get(new URI("hdfs://itcast01:9000"), new Configuration());
		
		InputStream in = fs.open(new Path("/jdk.avi"));
		
		FileOutputStream out = new FileOutputStream(new File("c:/jdk123456"));
		
		IOUtils.copyBytes(in, out, 2048, true);

	}

}
