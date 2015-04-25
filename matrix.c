#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#define N 1000                   
#define FROM_MASTER 1
#define FROM_SLAVE 2

int A[N][N], B[N][N];
unsigned long long C[N][N];

MPI_Status status;//消息接收状态变量,存储也是分布的	   

int main(int argc, char **argv)
{
    	int	process_num; //进程数,该变量为各处理器中的同名变量, 存储是分布的                   
	int	process_id;                     
	int	slave_num;                     
	int	dest; //目的进程标识号
	int	source; //发送数据进程的标识号
	int	rows;
	int	row_aver;
	int	remainder;                         
	int	offset;//行偏移量
	int	i, j, k;
                double     start_time, end_time;	   
	
	srand((unsigned int)time(NULL));
	
	for (i=0; i<N; i++)
	{
		for (j=0; j<N; j++)
		{
			A[i][j] = rand() % 10;
			B[i][j] = rand() % 10;
			C[i][k] = 0;
		}
	}
	
	MPI_Init(&argc, &argv);//初始化MPI

	/*该函数被各进程各调用一次,得到各自的进程id值*/
	MPI_Comm_rank(MPI_COMM_WORLD, &process_id);

	/*该函数被各进程各调用一次,得到进程数*/
	MPI_Comm_size(MPI_COMM_WORLD, &process_num);
	
	slave_num = process_num - 1;  
	
	if(process_id == 0)
	{
		row_aver = N / slave_num;
		remainder = N % slave_num;
		offset = 0;
		
//有的程序是将时间函数放在这个for循环的两边
		for(dest=1; dest<=slave_num; dest++) 
		{
			rows = (dest <= remainder) ? row_aver+1 : row_aver;
			printf("sending %d rows to process %d\n", rows, dest); 
			
			MPI_Send(&offset,            1, MPI_INT, dest, FROM_MASTER, MPI_COMM_WORLD);
			MPI_Send(&rows,              1, MPI_INT, dest, FROM_MASTER, MPI_COMM_WORLD);
			MPI_Send(&A[offset][0], rows*N, MPI_INT, dest, FROM_MASTER, MPI_COMM_WORLD);
			MPI_Send(&B,               N*N, MPI_INT, dest, FROM_MASTER, MPI_COMM_WORLD);
			
			offset += rows;
		}
		
		start_time = MPI_Wtime();
		
		for(source=1; source<=slave_num; source++)
		{
			MPI_Recv(&offset,     1, MPI_INT,  source, FROM_SLAVE, MPI_COMM_WORLD, &status); //接收行偏移量
			MPI_Recv(&rows,       1, MPI_INT,  source, FROM_SLAVE, MPI_COMM_WORLD, &status); //接收行数
			MPI_Recv(&C[offset][0], rows*N, MPI_UNSIGNED_LONG_LONG, source, FROM_SLAVE, MPI_COMM_WORLD, &status); //C接收从进程发回的结果
		}
		
		end_time = MPI_Wtime();

		printf("process cost %f seconds\n", end_time-start_time);
    }
	

    if(process_id > 0)
	{	
		MPI_Recv(&offset, 1, MPI_INT, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
		MPI_Recv(&rows,   1, MPI_INT, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
		MPI_Recv(&A, rows*N, MPI_INT, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
		MPI_Recv(&B,    N*N, MPI_INT, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
		
		for(i=0; i<rows; i++)
		{
			for (k=0; k<N; k++)
			{	
				int tmp = A[i][k];
				for (j=0; j<N; j++)
				{
					C[i][j] += tmp*B[k][j];	 
				}
			}	
		}
		
		MPI_Send(&offset, 1,           MPI_INT,		 0, FROM_SLAVE, MPI_COMM_WORLD);   //将行偏移量发回主进程
		MPI_Send(&rows,   1,           MPI_INT,		 0, FROM_SLAVE, MPI_COMM_WORLD);   //将行数发回主进程
		MPI_Send(&C, rows*N, MPI_UNSIGNED_LONG_LONG, 0, FROM_SLAVE, MPI_COMM_WORLD);   //将计算得到的值发回主进程
	}
	
	/*关闭MPI,标志并行代码段的结束*/
	MPI_Finalize();
	
	return 0;
}
