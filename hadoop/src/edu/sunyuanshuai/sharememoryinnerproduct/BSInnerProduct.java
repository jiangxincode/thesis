package edu.sunyuanshuai.sharememoryinnerproduct;
/*
 * <author>孙远帅</author>
 * <date>2012/10/26</date>
 * <email>sunyuanshuai@Gmail.com</email>
 */

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.filecache.*;

public class BSInnerProduct
{
	private static class BSInnerProductMap extends Mapper<IndexPair, DoubleWritable, IndexPair, DoubleWritable>
	{
		public void map(IndexPair key, DoubleWritable value, Context context) throws InterruptedException, IOException
		{
			context.write(key, value);
		}
	}
	
	private static class BSInnerPartitioner extends Partitioner<IndexPair, DoubleWritable>
	{
		public int getPartition(IndexPair key, DoubleWritable value, int numPartitions)
		{
			return key.x % numPartitions;
		}
	}
	
	private static class BSInnerGroupComparator extends WritableComparator
	{
		public BSInnerGroupComparator()
		{
			super(IndexPair.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			IndexPair ia = (IndexPair)w1;
			IndexPair ib = (IndexPair)w2;
			if(ia.x != ib.x)
			{
				return ia.x < ib.x ? -1 : 1;
			}
			return 0;
		}
	}
	
	private static class BSInnerReduce extends Reducer<IndexPair, DoubleWritable, IndexPair, DoubleWritable>
	{
		private int colNumLeftMatrix;
		private int rowNumRightMatrix;
		private int colNumRightMatrix;
		private int counter;
		private double sum;
		private double[][] rightMatrix;
		private double[] rowLeftMatrix;
		private IndexPair myKey = new IndexPair();
		private DoubleWritable myValue = new DoubleWritable();
		
		private void init(Configuration conf)
		{
			colNumLeftMatrix = Integer.valueOf(conf.getInt("BSInnerProduct.BSInnerReduce.ColNumLeftMatrix", 0));
			rowLeftMatrix = new double[colNumLeftMatrix];
			rowNumRightMatrix = Integer.valueOf(conf.getInt("BSInnerProduct.BSInnerReduce.RowNumRightMatrix", 0));
			colNumRightMatrix = Integer.valueOf(conf.getInt("BSInnerProduct.BSInnerReduce.ColNumRightMatrix", 0));
			rightMatrix = new double[rowNumRightMatrix][colNumRightMatrix];
		}
		
		private void getRightMatrixFormDistributedCache(Configuration conf)
		{
			try
			{
				Path[] cacheFilePaths = DistributedCache.getLocalCacheFiles(conf);
				File file = new File(cacheFilePaths[0].toString());
				File[] files = file.listFiles();
				for(int i =0; i < files.length; i++)
				{
					if(true == files[i].getName().contains("part-") && false == files[i].getName().contains("crc"))
					{
						System.out.println(files[i].getPath());
						FileSystem fs = FileSystem.get(URI.create(files[i].getPath()), conf);
						Path path = new Path(files[i].getPath());
						SequenceFile.Reader reader = null;
						try
						{
							reader = new SequenceFile.Reader(fs, path, conf);
							IndexPair key = (IndexPair)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
							DoubleWritable value = (DoubleWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
							while(reader.next(key, value))
							{
								rightMatrix[key.x][key.y] = value.get();
							}
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
						finally
						{
							IOUtils.closeStream(reader);
						}
					}
				}
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			init(conf);
			getRightMatrixFormDistributedCache(conf);
		}
		
		public void reduce(IndexPair key, Iterable<DoubleWritable> values, Context context) throws InterruptedException, IOException
		{
		    counter = 0;
			for(DoubleWritable dw : values)
			{
				rowLeftMatrix[counter] = dw.get();
				counter++;
			}
			
			for(int i = 0; i < colNumRightMatrix; i++)
			{
				sum = 0;
				for(int j = 0; j < rowNumRightMatrix; j++)
				{
					sum += rowLeftMatrix[j] * rightMatrix[j][i];
				}
				myKey.set(key.x, i);
				myValue.set(sum);
				context.write(myKey, myValue);
				//System.out.println(key.x + "\t" + i +"\t" + sum);
			}
		}
	}
	
	public static void runBSInner(int rowNumLeftMatrix, int colNumLeftMatrix, int rowNumRightMatrix, int colNumRightMatrix, String leftMatrixPath, String rightMatrixPath, String outputDir, int numReduceNum) throws Exception
	{
		if(colNumLeftMatrix != rowNumRightMatrix)
		{
			throw new Exception("the number of rows in left matrix is not equal to the number of columns in right matrix");
		}
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new Path(rightMatrixPath).toUri(), conf);
		conf.setInt("BSInnerProduct.BSInnerReduce.ColNumLeftMatrix", colNumLeftMatrix);
		conf.setInt("BSInnerProduct.BSInnerReduce.RowNumRightMatrix", rowNumRightMatrix);
		conf.setInt("BSInnerProduct.BSInnerReduce.ColNumRightMatrix", colNumRightMatrix);
		Job BSInnerJob = new Job(conf, "BSInnerJob");
		BSInnerJob.setJarByClass(BSInnerProduct.class);
		BSInnerJob.setMapperClass(BSInnerProductMap.class);
		BSInnerJob.setMapOutputKeyClass(IndexPair.class);
		BSInnerJob.setMapOutputValueClass(DoubleWritable.class);
		BSInnerJob.setReducerClass(BSInnerReduce.class);
		BSInnerJob.setOutputKeyClass(IndexPair.class);
		BSInnerJob.setOutputValueClass(DoubleWritable.class);
		BSInnerJob.setPartitionerClass(BSInnerPartitioner.class);
		BSInnerJob.setGroupingComparatorClass(BSInnerGroupComparator.class);
		BSInnerJob.setInputFormatClass(SequenceFileInputFormat.class);
		BSInnerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(BSInnerJob, new Path(leftMatrixPath));
		FileOutputFormat.setOutputPath(BSInnerJob, new Path(outputDir));
		BSInnerJob.setNumReduceTasks(numReduceNum);
		BSInnerJob.waitForCompletion(true);
	}
	
	/**
	 * @param args
	 */	
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
		int rowNumLeftMatrix = Integer.valueOf(args[0]);
		int colNumLeftMatrix = Integer.valueOf(args[1]);
		int rowNumRightMatrix = Integer.valueOf(args[2]);
		int colNumRightMatrix = Integer.valueOf(args[3]);
		String leftMatrixPath = args[4];
		String rightMatrixPath = args[5];
		String outputDir = args[6];
		int numReduceNum = Integer.valueOf(args[7]);
		try
		{
			runBSInner(rowNumLeftMatrix, colNumLeftMatrix, rowNumRightMatrix, colNumRightMatrix,  leftMatrixPath, rightMatrixPath, outputDir, numReduceNum);
			HDFSOperator.deleteDir(outputDir);
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
