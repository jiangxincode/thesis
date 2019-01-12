package edu.sunyuanshuai.innerproduct;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class InnerProduct {
	private static int colNumRightMatrix;

	private static class InnerKey implements WritableComparable<Object> {
		public int i;
		public int j;
		public int k;
		public int identify;

		public void write(DataOutput out) throws IOException {
			out.writeInt(i);
			out.writeInt(j);
			out.writeInt(k);
			out.writeByte(identify);
		}

		public void readFields(DataInput in) throws IOException {
			i = in.readInt();
			j = in.readInt();
			k = in.readInt();
			identify = in.readByte();
		}

		public void set(int i, int j, int k) {
			this.i = i;
			this.j = j;
			this.k = k;
		}

		public int compareTo(Object other) {
			InnerKey otherKey = (InnerKey) other;
			if (this.i < otherKey.i) {
				return -1;
			} else if (this.i > otherKey.i) {
				return 1;
			}

			if (this.j < otherKey.j) {
				return -1;
			} else if (this.j > otherKey.j) {
				return 1;
			}

			if (this.k < otherKey.k) {
				return -1;
			} else if (this.k > otherKey.k) {
				return 1;
			}

			if (this.identify < otherKey.identify) {
				return -1;
			} else if (this.identify > otherKey.identify) {
				return 1;
			}

			return 0;
		}

		public boolean equals(Object right) {
			return this.compareTo(right) == 0;
		}

		public String toString() {
			return this.i + "\t" + this.j + "\t" + this.k + "\t"
					+ this.identify;
		}
	}

	private static class InnerMap extends
			Mapper<IndexPair, DoubleWritable, InnerKey, DoubleWritable> {
		private boolean isLeftMatrix;
		private InnerKey myKey = new InnerKey();
		private int colNumRightMatrix;
		private int rowNumLeftMatrix;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String leftMatrixPath = ((FileSplit) inputSplit).getPath()
					.toString();
			isLeftMatrix = leftMatrixPath.indexOf(conf
					.get("InnerProduct.InnerMap.LeftMatrixPath")) != -1 ? true
					: false;
			colNumRightMatrix = conf.getInt(
					"InnerProduct.InnerMap.ColNumRightMatrix", 0);
			rowNumLeftMatrix = conf.getInt(
					"InnerProduct.InnerMap.RowNumLeftMatrix", 0);
		}

		public void map(IndexPair key, DoubleWritable value, Context context)
				throws InterruptedException, IOException {
			if (true == isLeftMatrix) {
				for (int j = 0; j < this.colNumRightMatrix; j++) {
					myKey.set(key.x, j, key.y);
					myKey.identify = 0;
					context.write(myKey, value);
				}
			} else {
				for (int i = 0; i < this.rowNumLeftMatrix; i++) {
					myKey.set(i, key.y, key.x);
					myKey.identify = 1;
					context.write(myKey, value);
				}
			}
		}
	}

	private static class InnerPartitioner extends
			Partitioner<InnerKey, DoubleWritable> {
		public int getPartition(InnerKey key, DoubleWritable value,
				int numPartitions) {
			return (key.i * InnerProduct.colNumRightMatrix + key.j)
					% numPartitions;
		}
	}

	private static class InnerGroupComparator extends WritableComparator {
		@SuppressWarnings("unused")
		protected InnerGroupComparator() {
			super(InnerKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable w1, WritableComparable w2) {
			InnerKey ik1 = (InnerKey) w1;
			InnerKey ik2 = (InnerKey) w2;
			if (ik1.i != ik2.i) {
				return ik1.i < ik2.i ? -1 : 1;
			} else if (ik1.j != ik2.j) {
				return ik1.j < ik2.j ? -1 : 1;
			}
			return 0;
		}
	}

	private static class InnerReduce extends
			Reducer<InnerKey, DoubleWritable, IndexPair, DoubleWritable> {
		private IndexPair myKey = new IndexPair();
		private DoubleWritable myValue = new DoubleWritable();
		private boolean flag;
		private double sum, tmp;

		public void reduce(InnerKey key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			myKey.set(key.i, key.j);
			sum = 0;
			flag = false;
			for (DoubleWritable dw : values) {
				if (false == flag) {
					tmp = dw.get();
					flag = true;
				} else {
					sum += tmp * dw.get();
					flag = false;
				}
			}
			myValue.set(sum);
			context.write(myKey, myValue);
			// System.out.println(myKey.x + "\t" + myKey.y + " : " +
			// myValue.toString());
		}
	}

	private static void runInnerProduct(int rowNumLeftMatrix,
			int colNumLeftMatrix, int rowNumRightMatrix, int colNumRightMatrix,
			String leftMatrixPath, String rightMatrixPath, String outputDir,
			int numReduce) throws Exception {
		if (colNumLeftMatrix != rowNumRightMatrix) {
			throw new Exception(
					"the number of rows in left matrix is not equal to the number of columns in right matrix");
		}
		InnerProduct.colNumRightMatrix = colNumRightMatrix;
		Configuration conf = new Configuration();
		conf.set("InnerProduct.InnerMap.LeftMatrixPath", leftMatrixPath);
		conf.setInt("InnerProduct.InnerMap.ColNumRightMatrix",
				colNumRightMatrix);
		conf.setInt("InnerProduct.InnerMap.RowNumLeftMatrix", rowNumLeftMatrix);
		System.out.println(conf.get("InnerProduct.InnerMap.LeftMatrixPath")
				+ "\t" + leftMatrixPath);
		@SuppressWarnings("deprecation")
		Job innerProductJob = new Job(conf, "InnerProduct");
		innerProductJob.setJarByClass(InnerProduct.class);
		innerProductJob.setMapperClass(InnerMap.class);
		innerProductJob.setReducerClass(InnerReduce.class);
		innerProductJob.setPartitionerClass(InnerPartitioner.class);
		innerProductJob.setGroupingComparatorClass(InnerGroupComparator.class);

		innerProductJob.setMapOutputKeyClass(InnerKey.class);
		innerProductJob.setMapOutputValueClass(DoubleWritable.class);

		innerProductJob.setOutputKeyClass(IndexPair.class);
		innerProductJob.setOutputValueClass(DoubleWritable.class);

		innerProductJob.setInputFormatClass(SequenceFileInputFormat.class);
		innerProductJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(innerProductJob, new Path(leftMatrixPath));
		FileInputFormat
				.addInputPath(innerProductJob, new Path(rightMatrixPath));
		FileOutputFormat.setOutputPath(innerProductJob, new Path(outputDir));
		innerProductJob.setNumReduceTasks(numReduce);
		innerProductJob.waitForCompletion(true);
	}

	/**
	 * @param args
	 *            : rowNumLeftMatrix, colNumLeftMatrix, rowNumRightMatrix,
	 *            colNumRightMatrix, leftMatrixPath, rightMatrixPath, outputDir,
	 *            numReduce
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int rowNumLeftMatrix = Integer.valueOf(args[0]);
		int colNumLeftMatrix = Integer.valueOf(args[1]);
		int rowNumRightMatrix = Integer.valueOf(args[2]);
		int colNumRightMatrix = Integer.valueOf(args[3]);
		String leftMatrixPath = args[4];
		String rightMatrixPath = args[5];
		String outputDir = args[6];
		int numReduce = Integer.valueOf(args[7]);
		runInnerProduct(rowNumLeftMatrix, colNumLeftMatrix, rowNumRightMatrix,
				colNumRightMatrix, leftMatrixPath, rightMatrixPath, outputDir,
				numReduce);
		System.out.println("begin delete the result data.................");
		HDFSOperator.deleteDir(outputDir);
	}

}
