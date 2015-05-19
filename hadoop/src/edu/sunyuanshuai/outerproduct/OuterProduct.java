package edu.sunyuanshuai.outerproduct;
/*
 * <author>孙远帅</author>
 * <date>2012/10/26</date>
 * <email>sunyuanshuai@Gmail.com</email>
 */

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class OuterProduct
{	
	private static class OuterKey implements WritableComparable
	{
		public int i;
		public int j;
		public byte identify;
		public void write(DataOutput out) throws IOException
		{
			out.writeInt(i);
			out.writeInt(j);
			out.writeByte(identify);
		}
		
		public void readFields(DataInput in) throws IOException
		{
			i = in.readInt();
			j = in.readInt();
			identify = in.readByte();
		}
		
		public void set(int i, int j, byte identify)
		{
			this.i = i;
			this.j = j;
			this.identify = identify;
		}
		
		public int compareTo(Object other)
		{
			OuterKey otherKey = (OuterKey)other;
			if(this.i != otherKey.i)
			{
				return this.i < otherKey.i ? -1 : 1;
			}
			
			if(this.identify != otherKey.identify)
			{
				return this.identify < otherKey.identify ? -1 : 1;
			}
			
			if(this.j != otherKey.j)
			{
				return this.j < otherKey.j ? -1 : 1;
			}			
			return 0;
		}
		public boolean equals(Object right) 
		{
			return this.compareTo(right) == 0;
		}
	}
	
	private static class OuterMap1 extends Mapper<IndexPair, DoubleWritable, OuterKey, DoubleWritable>
	{
		private boolean isLeftMatrix;
		private OuterKey myKey = new OuterKey();
		
		public void setup(Context context)
		{
			InputSplit inputSplit = context.getInputSplit();
			String leftMatrixPath = ((FileSplit)inputSplit).getPath().toString();
			Configuration conf = context.getConfiguration();
			isLeftMatrix = leftMatrixPath.indexOf(conf.get("OuterProduct.OuterMap1.LeftMatrixPath")) != -1 ? true : false;
		}
		
		public void map(IndexPair key, DoubleWritable value, Context context) throws InterruptedException, IOException
		{
			if(true == isLeftMatrix)
			{
				myKey.set(key.y, key.x, (byte)0);
			}
			else
			{
				myKey.set(key.x, key.y, (byte)1);
			}
			context.write(myKey, value);
		}
	}
	
	private static class OuterPartitioner1 extends Partitioner<OuterKey, DoubleWritable>
	{
		public int getPartition(OuterKey key, DoubleWritable value, int numPartitions)
		{
			return key.i % numPartitions;
		}
	}
	
	private static class OuterGroupComparator extends WritableComparator
	{
		protected OuterGroupComparator()
		{
			super(OuterKey.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			OuterKey ok1 = (OuterKey)w1;
			OuterKey ok2 = (OuterKey)w2;
			if(ok1.i != ok2.i)
			{
				return ok1.i < ok2.i ? -1 : 1;
			} 
			return 0;
		}
	}
	
	/*
	 * 输出格式：左矩阵列号\t左矩阵的列 + "#" + 右矩阵的对应行
	 */
	private static class OuterReduce1 extends Reducer<OuterKey, DoubleWritable, IntWritable, Text>
	{
		private IntWritable myKey = new IntWritable();
		private Text myValue = new Text();
		private StringBuffer rowRightMatrix = new StringBuffer();
		private StringBuffer colLeftMatrix = new StringBuffer();
		private int rowNumLeftMatrix, counterRowLeftMatrix;
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			this.rowNumLeftMatrix = conf.getInt("OuterProduct.OuterReduce1.RowNumLeftMatrix", 0);
		}
		
		public void reduce(OuterKey key, Iterable<DoubleWritable> values, Context context)  throws IOException, InterruptedException
		{
			colLeftMatrix.setLength(0);
			rowRightMatrix.setLength(0);
			counterRowLeftMatrix = 0;
			for(DoubleWritable dw : values)
			{
				if(counterRowLeftMatrix < this.rowNumLeftMatrix)
				{
					colLeftMatrix.append(dw + "\t");
					counterRowLeftMatrix++;
				}
				else
				{
					rowRightMatrix.append(dw + "\t");
				}
			}
			colLeftMatrix.append("#" + rowRightMatrix.toString());
			myValue.set(colLeftMatrix.toString());
			myKey.set(key.i);
			context.write(myKey, myValue);
		}
	}
	
	private static class OuterMap2 extends Mapper<IntWritable, Text, IndexPair, DoubleWritable>
	{
		private double[] colLeftMatrix;
		private double[] rowRightMatrix;
		private int rowNumLeftMatrix;
		private int colNumRightMatrix;
		private IndexPair myKey = new IndexPair();
		private DoubleWritable myValue = new DoubleWritable();
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			rowNumLeftMatrix = conf.getInt("OuterProduct.OuterMap2.RowNumLeftMatrix", 0);
			colNumRightMatrix = conf.getInt("OuterProduct.OuterMap2.ColNumRightMatrix", 0);
			colLeftMatrix = new double[rowNumLeftMatrix];
			rowRightMatrix = new double[colNumRightMatrix];			
		}
		
		public void map(IntWritable key, Text value, Context context) throws InterruptedException, IOException
		{
			String[] strs = value.toString().split("#");
			String[] elesColLeftMatrix = strs[0].trim().split("[ |\t]+");
			String[] elesRowRightMatrix = strs[1].trim().split("[ |\t]+");
			for(int i = 0; i < rowNumLeftMatrix; i++)
			{
				colLeftMatrix[i] = Double.valueOf(elesColLeftMatrix[i]);
			}
			
			for(int j = 0; j < colNumRightMatrix; j++)
			{
				rowRightMatrix[j] = Double.valueOf(elesRowRightMatrix[j]);
			}
			
			for(int i = 0; i < rowNumLeftMatrix; i++)
			{
				for(int j = 0; j < colNumRightMatrix; j++)
				{
					myKey.set(i, j);
					myValue.set(colLeftMatrix[i] * rowRightMatrix[j]);
					context.write(myKey, myValue);
				}
			}
		}
	}
	
	private static class OuterReduce2 extends Reducer<IndexPair, DoubleWritable, IndexPair, DoubleWritable>
	{
		private double sum;
		private DoubleWritable myValue = new DoubleWritable();
		public void reduce(IndexPair key, Iterable<DoubleWritable> values, Context context) throws InterruptedException, IOException
		{
			sum = 0;
			for(DoubleWritable value : values)
			{
				sum += Double.valueOf(value.toString());
			}
			myValue.set(sum);
			context.write(key, myValue);
		}
	}
	
	
	public static void runOuterProduct(int rowNumLeftMatrix, int colNumLeftMatrix, int rowNumRightMatrix, int colNumRightMatrix, String leftMatrixPath,
			String rightMatrixPath, String outputDir, int numReduce) throws Exception
	{
		if(colNumLeftMatrix != rowNumRightMatrix)
		{
			throw new Exception("the number of rows in left matrix is not equal to the number of columns in right matrix");
		}
		Configuration conf1 = new Configuration();
		conf1.set("OuterProduct.OuterMap1.LeftMatrixPath", leftMatrixPath);
		conf1.setInt("OuterProduct.OuterReduce1.RowNumLeftMatrix", rowNumLeftMatrix);
		Job outerProductJob1 = new Job(conf1, "OuterProduct");
		outerProductJob1.setJarByClass(OuterProduct.class);
		outerProductJob1.setMapperClass(OuterMap1.class);
		outerProductJob1.setMapOutputKeyClass(OuterKey.class);
		outerProductJob1.setMapOutputValueClass(DoubleWritable.class);
		
		outerProductJob1.setPartitionerClass(OuterPartitioner1.class);
		outerProductJob1.setGroupingComparatorClass(OuterGroupComparator.class);
		
		outerProductJob1.setReducerClass(OuterReduce1.class);
		outerProductJob1.setOutputKeyClass(IntWritable.class);
		outerProductJob1.setOutputValueClass(Text.class);
		outerProductJob1.setInputFormatClass(SequenceFileInputFormat.class);
		outerProductJob1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(outerProductJob1, new Path(leftMatrixPath));
		FileInputFormat.addInputPath(outerProductJob1, new Path(rightMatrixPath));
		FileOutputFormat.setOutputPath(outerProductJob1, new Path("Test/tmp/"));
		outerProductJob1.setNumReduceTasks(numReduce);
		outerProductJob1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		conf2.setInt("OuterProduct.OuterMap2.RowNumLeftMatrix", rowNumLeftMatrix);
		conf2.setInt("OuterProduct.OuterMap2.ColNumRightMatrix", colNumRightMatrix);
		Job outerProductJob2 = new Job(conf2, "OuterProduct2");
		outerProductJob2.setJarByClass(OuterProduct.class);
		outerProductJob2.setMapperClass(OuterMap2.class);
		outerProductJob2.setMapOutputKeyClass(IndexPair.class);
		outerProductJob2.setMapOutputValueClass(DoubleWritable.class);
		outerProductJob2.setReducerClass(OuterReduce2.class);
		outerProductJob2.setCombinerClass(OuterReduce2.class);
		outerProductJob2.setOutputKeyClass(IndexPair.class);
		outerProductJob2.setOutputValueClass(DoubleWritable.class);
		outerProductJob2.setInputFormatClass(SequenceFileInputFormat.class);
		outerProductJob2.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(outerProductJob2, new Path("Test/tmp/"));
		FileOutputFormat.setOutputPath(outerProductJob2, new Path(outputDir));
		outerProductJob2.setNumReduceTasks(numReduce);
		outerProductJob2.waitForCompletion(true);
		HDFSOperator.deleteDir("Test/tmp/");
	}
	
	/**
	 * @param args :  rowNumLeftMatrix, colNumLeftMatrix, rowNumRightMatrix, colNumRightMatrix, leftMatrixPath, rightMatrixPath, outputDir, numReduce
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		int rowNumLeftMatrix = Integer.valueOf(args[0]);
		int colNumLeftMatrix = Integer.valueOf(args[1]);
		int rowNumRightMatrix = Integer.valueOf(args[2]);
		int colNumRightMatrix = Integer.valueOf(args[3]);
		String leftMatrixPath = args[4];
		String rightMatrixPath = args[5];
		String outputDir = args[6];
		int numReduce = Integer.valueOf(args[7]);
		runOuterProduct(rowNumLeftMatrix, colNumLeftMatrix, rowNumRightMatrix, colNumRightMatrix, leftMatrixPath, rightMatrixPath, outputDir, numReduce);
		System.out.println("begin delete the result data.................");
		HDFSOperator.deleteDir(outputDir);
	}
}