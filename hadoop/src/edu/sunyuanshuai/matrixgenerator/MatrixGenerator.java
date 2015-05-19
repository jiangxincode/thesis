package edu.sunyuanshuai.matrixgenerator;
/*
 * <author>��Զ˧</author>
 * <date>2012/10/26</date>
 * <email>sunyuanshuai@Gmail.com</email>
 */
import java.util.Random;
import java.io.*;
import java.util.*;

public class MatrixGenerator
{
	/*
	 * get the random seed using the system current time 
	 */
	private static long getCurrentSeed()
	{
		return Calendar.getInstance().getTimeInMillis();
	}
	
	/*
	 * <func> generate the matrix stored with sparse format </func>
	 * <m> the number of generated matrix rows </m>
	 * <n> the number of generated matrix cloumns </n>
	 * <sparsity> the sparsity of generated matrix </sparsity>
	 * <sparseFileName> the name of sparse storage format </sparseFileName>
	 */
	private static int matrixGenerator_SparseStoration(int m, int n, double sparsity, String fileName)
	{	
		int eleNum = 0;
		try
		{
			Random rand = new Random(getCurrentSeed());
			FileWriter fw = new FileWriter(fileName);
			BufferedWriter bw = new BufferedWriter(fw);
			for(int i = 0; i < m; i++)
			{
				for(int j = 0; j < n; j++)
				{
					if(rand.nextDouble() <= sparsity)
					{
						bw.append(i + "\t" + j + "\t" + rand.nextDouble()*Integer.MAX_VALUE + "\r\n");
						eleNum++;
					}
				}
			}
			bw.close();
			fw.close();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			
		}
		return eleNum;
	}
	
	/**
	 * @param args
	 */		
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
		byte[] buffer = new byte[10000];
		try
		{
			int len = System.in.read(buffer);
			String[] params = new String(buffer).substring(0, len - 2).trim().split("[ |\t]+");
			boolean isSparse = Boolean.valueOf(params[0]);
			int m = Integer.valueOf(params[1]);
			int n = Integer.valueOf(params[2]);
			double sparsity = Double.valueOf(params[3]);
			if(isSparse)
			{
				System.out.println(matrixGenerator_SparseStoration(m, n, sparsity, params[4]));
				System.out.println(matrixGenerator_SparseStoration(n, m, sparsity, params[5]));
			}
			System.out.println("the generator finishes it.");
			
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
