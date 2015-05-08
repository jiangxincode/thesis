#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

int N;//数据个数
int K;//集合个数
int * CenterIndex;//初始化质心数组的索引
double * Center;//质心集合
double * CenterCopy;//质心集合副本
double * AllData;//数据集合
double ** Cluster;//簇的集合
int * Top;//集合中元素的个数，也会用作栈处理

//随机生成k个数x(0<=x<=n-1)作为起始的质心集合
void CreateRandomArray(int n, int k,int * center)
{
	int i=0, j = 0;
	for(i=0; i<k; ++i)  //随机生成k个数
	{
		int a=rand()%n;//判重

		for(j=0; j<i; j++)
			if(center[j]==a)//重复
				break;

		if(j>=i)//如果不重复，加入
			center[i]=a;
		else
			i--;//如果重复，本次重新随机生成
	}
}

//返回距离最小的质心的序号
int GetIndex(double value, double* center)
{
	int i;
	int index=0; //最小的质心序号
	double min = fabs(value-center[0]);//距质心最小距离
	for(i=1; i<K; i++)
	{
		double temp = fabs(value-center[i]);
		if(temp < min)//如果比当前距离还小，更新最小的质心序号和距离值
		{
			index=i;
			min=temp;
		}
	}
	return index;
}

void InitCenter()//初始化质心，随机生成法
{
	int i=0;
	CreateRandomArray(N,K,CenterIndex);//产生随机的K个<N的不同的序列
	for(i=0; i<K; i++)
	{
		Center[i]=AllData[CenterIndex[i]];//将对应数据赋值给质心数组
	}
	for(i=0; i<K; i++)//拷贝到质心副本
		CenterCopy[i]=Center[i];
}

//重新计算簇集合
void UpdateCluster()
{
	int i;
	int index;
	//将所有的集合清空，即将TOP置0
	for(i=0; i<K; i++)
		Top[i]=0;
	for(i=0; i<N; i++)
	{
		index=GetIndex(AllData[i],Center);//得到与当前数据最小的质心索引
		Cluster[index][Top[index]++]=AllData[i];//加入一个数据到一个Cluster[index]集合
	}
}

void UpdateCenter()//重新计算质心集合，对每一簇集合中的元素加总求平均即可
{
	int i=0;
	int j=0;
	double sum=0;
	for(i=0; i<K; i++)
	{
		sum=0;  //计算簇i的元素和

		for(j=0; j<Top[i]; j++)
			sum+=Cluster[i][j];
		if(Top[i]>0)//如果该簇元素不为空
			Center[i]=sum/Top[i];//求其平均值
	}
}

void InitData()//初始化聚类的各种数据
{
	char *filename = "data/kmeans.in";
	int i=0;
	FILE *pFile;
	pFile = fopen(filename, "rb");
	if(pFile == NULL)
	{
		fputs("File error\n", stderr);
		exit(1);
	}
	fread(&N, sizeof(int), 1, pFile);
	fread(&K, sizeof(int), 1, pFile);
	if(K>N)
	{
		exit(0);
	}
	Center=(double *)malloc(sizeof(double)*K);//为质心集合申请空间
	CenterCopy=(double *)malloc(sizeof(double)*K);//为质心集合副本申请空间
	CenterIndex=(int *)malloc(sizeof(int)*K);//为质心集合索引申请空间
	Top=(int *)malloc(sizeof(int)*K);
	AllData=(double *)malloc(sizeof(double)*N);//为数据集合申请空间
	Cluster=(double **)malloc(sizeof(double *)*K);//为簇集合申请空间

	for(i=0; i<K; i++)//初始化K个簇集合
	{
		Cluster[i]=(double *)malloc(sizeof(double)*N);
		Top[i]=0;
	}
	fread(AllData, sizeof(double), N, pFile);
	fclose(pFile);
	InitCenter();//初始化质心集合
	UpdateCluster();//初始化K个簇集合
}

/*
 * K均值算法：
 * 给定类的个数K，将N个对象分到K个类中去，
 * 使得类内对象之间的相似性最大，而类之间的相似性最小。
 */
int main()
{
	int i,j;
	int flag=1; //迭代标志，若为false,则迭代结束
	InitData(); //初始化数据
	while(flag) //开始迭代
	{
		UpdateCluster(); //更新各个聚类
		UpdateCenter(); //更新质心数组
		if(memcmp(Center, CenterCopy, sizeof(double)*K) == 0) //如果本次迭代与前次的质心聚合相等，即已收敛，结束退出
		{
			flag=0;
		}
		else //否则将质心副本置为本次迭代得到的的质心集合
		{
			for(i=0; i<K; i++)//拷贝到质心副本
			{
				CenterCopy[i]=Center[i];
			}
		}
	}
	for(i=0; i<K; i++)
	{
		printf("\n第%d组: 质心（%f） ",i,Center[i]);
		for(j=0; j<Top[i]; j++)
		{
			printf("%f ",Cluster[i][j]);
		}
	}
	return 0;
}
