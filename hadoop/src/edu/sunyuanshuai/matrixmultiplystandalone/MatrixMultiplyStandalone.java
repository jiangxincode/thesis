package edu.sunyuanshuai.matrixmultiplystandalone;
/*
 * <author>��Զ˧</author>
 * <date>2012/10/26</date>
 * <email>sunyuanshuai@Gmail.com</email>
 */
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MatrixMultiplyStandalone
{
	
	private double[][] leftMatrix;
	private double[][] rightMatrix;
	private int rowNumLeftMatrix, colNumLeftMatrix, rowNumRightMatrix, colNumRightMatrix;
	FileReader fr;
	BufferedReader br;

	public static String nowTime()
	{
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		return dateFormat.format(c.getTime()); 
	}
	
	private void openBufferedReader(String fileName)
	{
		try
		{
			fr = new FileReader(fileName);
			br = new BufferedReader(fr);
		} catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void readLeftMatrix(String fileName)
	{
		openBufferedReader(fileName);
		String line;
		for(int i = 0; i < rowNumLeftMatrix; i++)
		{
			for(int j = 0; j < colNumLeftMatrix; j++)
			{
				leftMatrix[i][j] = 0;
			}
		}
		int rowIndex, colIndex;
		try
		{
			if(br.ready())
			{
				while((line = br.readLine()) != null)
				{
					String[] info = line.split("[ |\t]+");
					rowIndex = Integer.valueOf(info[0]);
					colIndex = Integer.valueOf(info[1]);
					leftMatrix[rowIndex][colIndex] = Double.valueOf(info[2]);
				}
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			closeBufferedReader();
		}
	}

	public void readRightMatrix(String fileName)
	{
		openBufferedReader(fileName);
		String line;
		for(int i = 0; i < rowNumRightMatrix; i++)
		{
			for(int j = 0; j < colNumRightMatrix; j++)
			{
				rightMatrix[i][j] = 0;
			}
		}
		int rowIndex, colIndex;
		try
		{
			if(br.ready())
			{
				while((line = br.readLine()) != null)
				{
					String[] info = line.split("[ |\t]+");
					rowIndex = Integer.valueOf(info[0]);
					colIndex = Integer.valueOf(info[1]);
					rightMatrix[rowIndex][colIndex] = Double.valueOf(info[2]);
				}
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			closeBufferedReader();
		}
	}
	
	private void closeBufferedReader()
	{
		try
		{
			br.close();
			fr.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	private void multiply(String fileName)
	{
		try
		{
			FileWriter fw = new FileWriter(fileName);
			BufferedWriter bw = new BufferedWriter(fw);
			double cij;
			for(int i = 0; i < rowNumLeftMatrix; i++)
			{
				for(int j = 0; j < colNumRightMatrix; j++)
				{
					cij = 0;
					for(int k = 0; k < colNumLeftMatrix; k++)
					{
						cij += leftMatrix[i][k] * rightMatrix[k][j];
					}
					bw.append(i + "\t" + j + "\t" + cij + "\r\n");
				}
			}
			bw.close();
			fw.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	public MatrixMultiplyStandalone()
	{
		rowNumLeftMatrix  = 0;
		colNumLeftMatrix = 0;
		rowNumRightMatrix = 0;
		colNumRightMatrix = 0;
	}
	
	public MatrixMultiplyStandalone(int rowNumLeftMatrix, int colNumLeftMatrix, int rowNumRightMatrix, int colNumRightMatrix) throws Exception
	{
		if(colNumLeftMatrix != rowNumRightMatrix)
		{
			throw new Exception("the number of left matrix cloumns is not equal to the number of right matrix rows!");
		}
		this.rowNumLeftMatrix = rowNumLeftMatrix;
		this.colNumLeftMatrix = colNumLeftMatrix;
		this.rowNumRightMatrix = rowNumRightMatrix;
		this.colNumRightMatrix = colNumRightMatrix;
		this.leftMatrix = new double[this.rowNumLeftMatrix][this.colNumLeftMatrix];
		this.rightMatrix = new double[this.rowNumRightMatrix][this.colNumRightMatrix];
	}

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
		try
		{
			System.out.println("1k*1k*1k begining.....");
			System.out.println("begin time: " + nowTime());
			MatrixMultiplyStandalone mulAloneInstance = new MatrixMultiplyStandalone(1000, 1000, 1000, 1000);
			System.out.println("1k begin time");
			long begin1k = System.currentTimeMillis();
			mulAloneInstance.readLeftMatrix("test1kl.txt");
			mulAloneInstance.readRightMatrix("test1kr.txt");
			mulAloneInstance.multiply("result1k.txt");
			System.out.println("end time: " + nowTime());
			System.out.println("1k need time : " + (System.currentTimeMillis() - begin1k));
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
