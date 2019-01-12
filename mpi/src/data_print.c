#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "MyMPI.h"

int kmeans_print(char *filename, int extra);
int matrix_print(char *filename, int extra);

int main(int argc, char **argv)
{
	char *filename = DEFAULT_FILENAME;
	int extra = 0; //默认不输出额外参数
	char type[BUFSIZ];
	opterr = 0;
	int opt;
	while((opt = getopt(argc, argv, "et:f:")) != EOF)
	{
		switch(opt)
		{
		case 't':
			strncpy(type, optarg, BUFSIZ);
			break;
		case 'e':
			extra = 1;
			break;
		case 'f':
			filename = optarg;
			break;
		case '?':
			fprintf(stderr, "Unknown option '-%c'\n", optopt);
			exit(1);
		}
	}
	if(strncmp(type, "matrix", BUFSIZ) == 0)
	{
		matrix_print(filename, extra);
	}
	else if(strncmp(type, "kmeans", BUFSIZ) == 0)
	{
		kmeans_print(filename, extra);
	}
	else
	{
		perror("invalid type!");
		exit(1);
	}
	return 0;
}

int kmeans_print(char *filename, int extra)
{
	FILE *pFile;
	int N;
	int K;
	dtype* data;
	int i;

	pFile = fopen(filename, "rb");
	if(pFile == NULL)
	{
		fputs("File error\n", stderr);
		exit(1);
	}

	rewind(pFile);

	fread(&N, sizeof(int), 1, pFile);
	fread(&K, sizeof(int), 1, pFile);
	if(extra)
	{
		printf("N: %d\tK: %d\n", N, K);
	}
	data = (dtype *)malloc(sizeof(dtype)*N);
	fread(data, sizeof(dtype), N, pFile);
	for(i = 0; i < N; i++)
	{
		printf("%f ", data[i]);
	}
	printf("\n");
	fclose(pFile);
	free(data);
	return 0;
}

int matrix_print(char *filename, int extra)
{
	FILE *pFile;
	int row;
	int column;
	dtype** matrix;
	int i, j;

	pFile = fopen(filename, "rb");
	if(pFile == NULL)
	{
		fputs("File error\n", stderr);
		exit(1);
	}

	rewind(pFile);

	fread(&row, sizeof(int), 1, pFile);
	fread(&column, sizeof(int), 1, pFile);
	if(extra)
	{
		printf("row: %d\tcolumn: %d\n", row, column);
	}

	matrix = (dtype **)malloc(row * PTR_SIZE);
	for(i=0; i<row; i++)
	{
		*(matrix+i) = (dtype*)malloc(column * sizeof(dtype));
	}
	for(i=0; i<(row); i++)
	{
		fread(matrix[i], sizeof(dtype), column, pFile);
	}

	for(i = 0; i < row; i++)
	{
		for(j = 0; j < column; j++)
		{
			printf("%f ", matrix[i][j]);
		}
		printf("\n");
	}

	fclose(pFile);
	for(i = 0; i < row; i++)
	{
		free(matrix[i]);
	}
	free(matrix);

	return 0;
}
