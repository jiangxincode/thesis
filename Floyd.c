//Floyd算法解决“圈点最短路径问题”

#include <stdio.h>
#include <mpi.h>
#include "MyMPI.h"

//定义矩阵类型
typedef int dtype;
#define MPI_TYPE MPI_INT	

void compute_shortest_paths(int id,int p,dtype **a,int n);
int main(int argc,char *argv[])
{
	//main函数读取文件、打印初始距离矩阵，调用最短路径函数并打印结果矩阵
	dtype **a;//二级指针
	dtype* storage;//数组元素	
	int i,j,k;
	int id;//进程号
	int m;//矩阵行
	int n;//矩阵列
	int p;//进程数
	void compute_shortest_paths(int,int,int**,int);//计算最短路径
	MPI_Comm_rank(MPI_COMM_WORLD,&id);
	MPI_Comm_size(MPI_COMM_WORLD,&p);
	read_row_striped_matrix(argv[1],(void *)&a,(void *)&storage,//矩阵的输入函数
		MPI_TYPE,&m,&n,MPI_COMM_WORLD);
	if(m!=i)
		terminate(id,"必须为方阵！！！\n");
	print_row_striped_matrix((void ** )a,MPI_TYPE,m,n,MPI_COMM_WORLD);//输出原矩阵
	compute_shortest_paths(id,p,(dtype **)a,n);
	print_row_striped_matrix((void **) a,MPI_TYPE,m,n,MPI_COMM_WORLD);//输出结果矩阵
	MPI_Finalize();
}

void compute_shortest_paths(int id,int p,dtype **a,int n)
{
	//求最短路径
	int i,j,k;
	int offset;//广播行标记
	int root;//控制行广播的进程
	int *tmp;//暂存广播的行
	tmp=(dtype *)malloc(n*sizeof(dtype));
	for(k=0;k<n;k++)
	{
		root=BLOCK_OWNER(k,p,n);
		if(root==id){
			offset=k-BLOCK_LOW(id,p,n);
			for(j=0;j<n;j++)
				tmp[j]=a[offset][j];
		}
		MPI_Bcast(tmp,n,MPI_TYPE,root,MPI_COMM_WORLD);
		for(i=0;i<10;i++)
			for(j=0;j<n;j++)
				a[i][j]=MIN(a[i][j],a[i][k]+tmp[j]);
	}
	free(tmp);
		
}