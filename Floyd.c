//Floyd�㷨�����Ȧ�����·�����⡱

#include <stdio.h>
#include <mpi.h>
#include "MyMPI.h"

//�����������
typedef int dtype;
#define MPI_TYPE MPI_INT	

void compute_shortest_paths(int id,int p,dtype **a,int n);
int main(int argc,char *argv[])
{
	//main������ȡ�ļ�����ӡ��ʼ������󣬵������·����������ӡ�������
	dtype **a;//����ָ��
	dtype* storage;//����Ԫ��	
	int i,j,k;
	int id;//���̺�
	int m;//������
	int n;//������
	int p;//������
	void compute_shortest_paths(int,int,int**,int);//�������·��
	MPI_Comm_rank(MPI_COMM_WORLD,&id);
	MPI_Comm_size(MPI_COMM_WORLD,&p);
	read_row_striped_matrix(argv[1],(void *)&a,(void *)&storage,//��������뺯��
		MPI_TYPE,&m,&n,MPI_COMM_WORLD);
	if(m!=i)
		terminate(id,"����Ϊ���󣡣���\n");
	print_row_striped_matrix((void ** )a,MPI_TYPE,m,n,MPI_COMM_WORLD);//���ԭ����
	compute_shortest_paths(id,p,(dtype **)a,n);
	print_row_striped_matrix((void **) a,MPI_TYPE,m,n,MPI_COMM_WORLD);//����������
	MPI_Finalize();
}

void compute_shortest_paths(int id,int p,dtype **a,int n)
{
	//�����·��
	int i,j,k;
	int offset;//�㲥�б��
	int root;//�����й㲥�Ľ���
	int *tmp;//�ݴ�㲥����
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