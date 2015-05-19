package edu.sunyuanshuai.blockproduct.sparsematrixproduct;
/*
 * <author>孙远帅</author>
 * <date>2012/10/26</date>
 * <email>sunyuanshuai@Gmail.com</email>
 */
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class SparseMatrixMultiply 
{	
	/**	Global variables for the mapper and reducer tasks. */
	
	private static String LeftMatrix;
	private static int I;
	private static int K;
	private static int J;
	private static int IB;
	private static int KB;
	private static int JB;
	
	private static int NIB;
	private static int NKB;
	private static int NJB;
	
	private static int lastIBlockNum;
	private static int lastIBlockSize;
	private static int lastKBlockNum;
	private static int lastKBlockSize;
	private static int lastJBlockNum;
	private static int lastJBlockSize;
	
	private static Counter mulDatablockNum = null;
	
	/**	The key class for the input and output sequence files and the job 2 intermediate keys. */
	
	/**	The job 1 intermediate key class. */
	
	private static class Key implements WritableComparable
	{
		public int index1;
		public int index2;
		public int index3;
		public byte m;
		public void write (DataOutput out) throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
			out.writeInt(index3);
			out.writeByte(m);
		}
		public void readFields (DataInput in) throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
			index3 = in.readInt();
			m = in.readByte();
		}
		public int compareTo (Object other) 
		{
			Key o = (Key)other;
			if(this.index1 != o.index1)
			{
				return (this.index1 < o.index1 ? -1 : 1);
			}			
			if(this.index2 != o.index2)
			{
				return (this.index2 < o.index2 ? -1 : 1);
			}			
			if(this.index3 != o.index3)
			{
				return (this.index3 < o.index3 ? -1 : 1);
			}
			if(this.m != o.m)
			{
				return (this.m < o.m ? -1 : 1);
			}
			return 0;
		}
	}
	
	/**	The job 1 intermediate value class. */
	
	private static class Value implements Writable 
	{
		public int index1;
		public int index2;
		public double v;
		public void write (DataOutput out)
			throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
			out.writeDouble(v);
		}
		public void readFields (DataInput in) 	throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
			v = in.readDouble();
		}
	}
	
	/**	The job 1 mapper class. */
	
	private static class Job1Mapper extends Mapper<IndexPair, DoubleWritable, Key, Value>
	{
		private Path path;
		private boolean matrixA;
		private Key key = new Key();
		private Value value = new Value();
		
		public void setup (Context context)
		{
			init(context);
			FileSplit split = (FileSplit)context.getInputSplit();
			path = split.getPath();
			matrixA = path.toString().startsWith(LeftMatrix);
		}
		
		public void map (IndexPair indexPair, DoubleWritable el, Context context) throws IOException, InterruptedException 
		{
			int i = 0;
			int k = 0;
			int j = 0;
			if (matrixA) 
			{
				i = indexPair.x;
				k = indexPair.y;
			} else
			{
				k = indexPair.x;
				j = indexPair.y;
			}
			value.v = el.get();
			if (matrixA) 
			{
				key.index1 = i/IB;
				key.index2 = k/KB;
				key.m = 0;
				value.index1 = i % IB;
				value.index2 = k % KB;
				for (int jb = 0; jb < NJB; jb++)
				{
					key.index3 = jb;
					context.write(key, value);
				}
			} 
			else
			{
				key.index2 = k/KB;
				key.index3 = j/JB;
				key.m = 1;
				value.index1 = k % KB;
				value.index2 = j % JB;
				for (int ib = 0; ib < NIB; ib++)
				{
					key.index1 = ib;
					context.write(key, value);
				}
			}
		}
	}
	
	/**	The job 1 partitioner class. */
	
	private static class Job1Partitioner 	extends Partitioner<Key, Value>
	{
		public int getPartition (Key key, Value value, int numPartitions)
		{
			int kb, ib, jb;
			ib = key.index1;
			kb = key.index2;
			jb = key.index3;
			return ((ib*NKB + kb)*NJB + jb) % numPartitions;
		}
	}
	
	/*the unit of sparse matrix*/
	public static class Point
	{
		private int index;
		private double value;
		public Point() {}
		public Point(int index, double value)
		{
			this.index = index;
			this.value = value;
		}
	}
	
	public static class Job1Reducer extends Reducer<Key, Value, IndexPair, DoubleWritable>
	{
		private Point[][] A;
		private double[][] B;
		private int sib, skb, sjb;
		private int[] numOfEleA;
		private int aRowDim, aColDim, bColDim;
		private IndexPair indexPair = new IndexPair();
		private DoubleWritable el = new DoubleWritable();
		
		public void setup (Context context) 
		{
			init(context);
			A = new Point[IB][KB];
			B = new double[KB][JB];
			sib = -1;
			skb = -1;
			sjb = -1;
			numOfEleA = new int[IB];
		}
		
		private int getDim (int blockNum, int lastBlockNum, int blockSize, 	int lastBlockSize) 
		{
			return blockNum < lastBlockNum ? blockSize : lastBlockSize;
		}
		
		private void build (double[][] matrix, int rowDim, int colDim,  Iterable<Value> valueList) 
		{
			for (int rowIndex = 0; rowIndex < rowDim; rowIndex++)
			{
				for (int colIndex = 0; colIndex < colDim; colIndex++)
				{
					matrix[rowIndex][colIndex] = 0;
				}
			}
			for (Value value : valueList)
			{
				matrix[value.index1][value.index2] = value.v;
			}
		}
		
		private void build (Point[][] matrix, int rowDim, int colDim,  Iterable<Value> valueList) 
		{
			for(int i = 0; i < IB; i++)
			{
				numOfEleA[i] = 0;
			}
			for (Value value : valueList)
			{
				matrix[value.index1][numOfEleA[value.index1]] = new Point(value.index2, value.v);
				numOfEleA[value.index1] = numOfEleA[value.index1] + 1;				
			}
		}
		
		private void multiplyAndEmit (Context context, int ib, int jb) throws IOException, InterruptedException 
		{
			int ibase = ib*IB;
			int jbase = jb*JB;
			for (int i = 0; i < aRowDim; i++) 
			{
				for (int j = 0; j < bColDim; j++)
				{
					double sum = 0;
					for (int k = 0; k < numOfEleA[i]; k++) 
					{
						sum += A[i][k].value * B[A[i][k].index][j];
					}
					if (sum != 0)
					{
						indexPair.x = ibase + i;
						indexPair.y = jbase + j;
						el.set(sum);
						context.write(indexPair, el);
					}
				}
			}
		}
	
		public void reduce (Key key, Iterable<Value> valueList, Context context) throws IOException, InterruptedException 
		{
			int ib, kb, jb;
			ib = key.index1;
			kb = key.index2;
			jb = key.index3;
			if (key.m == 0) 
			{
				sib = ib;
				skb = kb;
				sjb = jb;
				aRowDim = getDim(ib, lastIBlockNum, IB, lastIBlockSize);
				aColDim = getDim(kb, lastKBlockNum, KB, lastKBlockSize);
				build(A, aRowDim, aColDim, valueList);
			} else
			{
				if (ib != sib || kb != skb || jb != sjb) 
				{
					mulDatablockNum = context.getCounter("MyCounter", "throwDatablockNum");
					mulDatablockNum.increment(1);
					return;
				}
				bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
				build(B, aColDim, bColDim, valueList);
				mulDatablockNum = context.getCounter("MyCounter", "mulDatablockNum");
				mulDatablockNum.increment(1);
				multiplyAndEmit(context, ib, jb);
			}
		}	
	}
	
	private static class Job2Map extends Mapper<IndexPair, DoubleWritable, IndexPair, DoubleWritable>
	{
		public void map(IndexPair key, DoubleWritable value, Context context) throws InterruptedException, IOException
		{
			context.write(key, value);
		}
	}
	
	private static class Job2Reducer extends Reducer<IndexPair, DoubleWritable, IndexPair, DoubleWritable>
	{
		private double sum;
		private DoubleWritable myValue = new DoubleWritable();
		public void reduce(IndexPair key, Iterable<DoubleWritable> values, Context context) throws InterruptedException, IOException
		{
			sum = 0;
			for(DoubleWritable value : values)
			{
				sum += value.get();
			}
			myValue.set(sum);
			//System.out.println(key.x + "\t" + key.y + " \t " + sum);
			context.write(key, myValue);
		}
	}
	
	
	/**	Initializes the global variables from the job context for the mapper and reducer tasks. */
	
	private static void init (JobContext context) 
	{
		Configuration conf = context.getConfiguration();
		LeftMatrix = conf.get("MatrixMultiply.LeftMatrix");
		I = conf.getInt("MatrixMultiply.I", 0);
		K = conf.getInt("MatrixMultiply.K", 0);
		J = conf.getInt("MatrixMultiply.J", 0);
		IB = conf.getInt("MatrixMultiply.IB", 0);
		KB = conf.getInt("MatrixMultiply.KB", 0);
		JB = conf.getInt("MatrixMultiply.JB", 0);
		NIB = (I-1)/IB + 1;
		NKB = (K-1)/KB + 1;
		NJB = (J-1)/JB + 1;
		lastIBlockNum = NIB-1;
		lastIBlockSize = I - lastIBlockNum*IB;
		lastKBlockNum = NKB-1;
		lastKBlockSize = K - lastKBlockNum*KB;
		lastJBlockNum = NJB-1;
		lastJBlockSize = J - lastJBlockNum*JB;
	}
	
	/**	Configures and runs job 1. */
	
	private static void job1 (Configuration conf) 	throws Exception
	{
		Job job = new Job(conf, "Matrix Multiply Job 1");
		job.setJarByClass(SparseMatrixMultiply.class);
		job.setNumReduceTasks(conf.getInt("MatrixMultiply.R1", 0));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);
		job.setPartitionerClass(Job1Partitioner.class);	
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		job.setOutputKeyClass(IndexPair.class);
		job.setOutputValueClass(DoubleWritable.class);		
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.LeftMatrix")));
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.RightMatrix")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.tempDirPath")));
		boolean ok = job.waitForCompletion(true);
		if (!ok) throw new Exception("Job 1 failed");
	}
	
	/**	Configures and runs job 2. */
	
	private static void job2 (Configuration conf) throws Exception
	{
		Job job = new Job(conf, "Matrix Multiply Job 2");
		job.setJarByClass(SparseMatrixMultiply.class);
		job.setNumReduceTasks(conf.getInt("MatrixMultiply.R2", 0));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(Job2Map.class);
		job.setCombinerClass(Job2Reducer.class);
		job.setReducerClass(Job2Reducer.class);
		job.setOutputKeyClass(IndexPair.class);
		job.setOutputValueClass(DoubleWritable.class);		
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.tempDirPath")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.outputDirPath")));
		boolean ok = job.waitForCompletion(true);
		if (!ok) throw new Exception("Job 2 failed");
	}
	
	/**	Runs a matrix multiplication job.
	 *
	 *	<p>This method is thread safe, so it can be used to run multiple concurrent
	 *	matrix multiplication jobs, provided each concurrent invocation uses a separate
	 *	configuration.
	 *
	 *	<p>The input and output files are sequence files, with key class 
	 *	MatrixMultiply.IndexPair and value class IntWritable.
	 *
	 *	@param	conf			The configuration.
	 *
	 *	@param	LeftMatrix		Path to input file or directory of input files for matrix A.
	 *
	 *	@param	RightMatrix		Path to input file or directory of input files for matrix B.
	 *
	 *	@param	outputDirPath	Path to directory of output files for C = A*B. This directory 
	 *							is deleted if it already exists.
	 *
	 *	@param	tempDirPath		Path to directory for temporary files. A subdirectory
	 *							of this directory named "MatrixMultiply-nnnnn" is created
	 *							to hold the files that are the output of job 1 and the
	 *							input of job 2, where "nnnnn" is a random number. This 
	 *							subdirectory is deleted before the method returns. 
	 *
	 *	@param	R1				Number of reduce tasks for job 1.
	 *
	 *	@param	R2				Number of reduce tasks for job 2. Only used for strategies
	 *							1, 2, and 3.
	 *
	 *	@param	I				Row dimension of matrix A and matrix C.
	 *
	 *	@param	K				Column dimension of matrix A and row dimension of matrix B.
	 *
	 *	@param	J				Column dimension of matrix A and matrix C.
	 *
	 *	@param	IB				Number of rows per A block and C block.
	 *
	 *	@param	KB				Number of columns per A block and rows per B block.
	 *
	 *	@param	JB				Number of columns per B block and C block.
	 *
	 *	@throws	Exception
	 */
	
	public static void runJob (Configuration conf, String LeftMatrix, String RightMatrix, String outputDirPath, String tempDirPath, int R1, int R2,
		int I, int K, int J, int IB, int KB, int JB) 	throws Exception
	{
		FileSystem fs = FileSystem.get(conf);
		LeftMatrix = fs.makeQualified(new Path(LeftMatrix)).toString();
		RightMatrix = fs.makeQualified(new Path(RightMatrix)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();
		tempDirPath = fs.makeQualified(new Path(tempDirPath)).toString();
		tempDirPath = tempDirPath + "/MatrixMultiply-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));        	
		conf.set("MatrixMultiply.LeftMatrix", LeftMatrix);
		conf.set("MatrixMultiply.RightMatrix", RightMatrix);
		conf.set("MatrixMultiply.outputDirPath", outputDirPath);
		conf.set("MatrixMultiply.tempDirPath", tempDirPath);
		conf.setInt("MatrixMultiply.R1", R1);
		conf.setInt("MatrixMultiply.R2", R2);
		conf.setInt("MatrixMultiply.I", I);
		conf.setInt("MatrixMultiply.K", K);
		conf.setInt("MatrixMultiply.J", J);
		conf.setInt("MatrixMultiply.IB", IB);
		conf.setInt("MatrixMultiply.KB", KB);
		conf.setInt("MatrixMultiply.JB", JB);		
		fs.delete(new Path(tempDirPath), true);
		fs.delete(new Path(outputDirPath), true);
		
		try 
		{
			job1(conf);
			job2(conf);
		} finally 
		{
			fs.delete(new Path(tempDirPath), true);
		}
	}
	
	/**	Prints a usage error message and exits. */
	
	private static void printUsageAndExit ()
	{
		System.err.println("Usage: MatrixMultiply [generic args] LeftMatrix RightMatrix " +
			"outputDirPath tempDirPath strategy R1 R2 I K J IB KB JB");
		System.exit(2);
	}
	
	/**	Main program.
	 *
	 *	<p>Usage:
	 *
	 *	<p><code>MatrixMultiply [generic args] LeftMatrix RightMatrix
	 *		outputDirPath tempDirPath strategy R1 R2 I K J IB KB JB</code>
	 *
	 *	@param	args		Command line arguments.
	 *
	 *	@throws Eception
	 */
	 
	public static void main (String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String LeftMatrix = args[0];
		String RightMatrix = args[1];
		String outputDirPath = args[2];
		String tempDirPath = args[3];
		int R1 = 0;
		int R2 = 0;
		int I = 0;
		int K = 0;
		int J = 0;
		int IB = 0;
		int KB = 0;
		int JB = 0;
		try
		{
			R1 = Integer.parseInt(args[4]);
			R2 = Integer.parseInt(args[5]);
			I = Integer.parseInt(args[6]);
			K = Integer.parseInt(args[7]);
			J = Integer.parseInt(args[8]);
			IB = Integer.parseInt(args[9]);
			KB = Integer.parseInt(args[10]);
			JB = Integer.parseInt(args[11]);
		} catch (NumberFormatException e) 
		{
			System.err.println("Syntax error in integer argument");
			printUsageAndExit();
		}
		runJob(conf, LeftMatrix, RightMatrix, outputDirPath, tempDirPath, R1, R2, I, K, J, IB, KB, JB);
		
		System.out.println("begin delete the result data.................");
		HDFSOperator.deleteDir(outputDirPath);
		HDFSOperator.deleteDir(tempDirPath);
	}
}