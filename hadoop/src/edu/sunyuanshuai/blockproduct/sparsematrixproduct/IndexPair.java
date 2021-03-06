package edu.sunyuanshuai.blockproduct.sparsematrixproduct;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


@SuppressWarnings("rawtypes")
public class IndexPair implements WritableComparable 
	{
		public int x;
		public int y;
		public void write (DataOutput out) throws IOException
		{
			out.writeInt(x);
			out.writeInt(y);
		}
		public void readFields (DataInput in) 	throws IOException
		{
			x = in.readInt();
			y = in.readInt();
		}
		
		public void set(int x, int y)
		{
			this.x = x;
			this.y = y;
		}
		
		public int compareTo (Object other) 
		{
			IndexPair otherIndexPair = (IndexPair)other;
			if (this.x < otherIndexPair.x)
			{
				return -1;
			} else if (this.x > otherIndexPair.x)
			{
				return 1;
			}
			
			if (this.y < otherIndexPair.y) 
			{
				return -1;
			} else if (this.y > otherIndexPair.y)
			{
				return 1;
			}
			return 0;
		}
		
		public int hashCode()
		{
			return this.x << 30 + this.y;
		}
		
		public String toString()
		{
			return this.x + "\t" + this.y;
		}
	}