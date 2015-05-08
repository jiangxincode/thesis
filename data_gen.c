#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include "MyMPI.h"

void matrix_write(char *filename, int row, int column);
void kmeans_write(char *filename, int N, int K);

int main(int argc, char *argv[])
{
	char type[BUFSIZ] = DEFAULT_TYPE;
	int row = DEFAULT_ROW;
	int column = DEFAULT_COLUMN;
	int K = DEFAULT_K;
	int N = DEFAULT_N;
	char *filename = DEFAULT_FILENAME;

	opterr = 0;
	int opt;
	while((opt = getopt(argc, argv, "r:c:f:K:N:t:")) != EOF)
	{
		switch(opt)
		{
		case 't':
			strncpy(type, optarg, BUFSIZ);
			break;
		case 'r':
			row = atoi(optarg);
			break;
		case 'c':
			column = atoi(optarg);
			break;
		case 'K':
			K = atoi(optarg);
			break;
		case 'N':
			N = atoi(optarg);
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
		matrix_write(filename, row, column);
	}
	else if(strncmp(type, "kmeans", BUFSIZ) == 0)
	{
		kmeans_write(filename, N, K);
	}
	else
	{
		perror("invalid type!");
		exit(1);
	}
	return 0;
}

void kmeans_write(char *filename, int N, int K)
{
	int i;
	FILE *pFile = fopen(filename, "wb");
	if(pFile == NULL)
	{
		fputs("File error\n", stderr);
		exit(1);
	}

	dtype *data = (dtype *)malloc(N * sizeof(dtype));
	if(data == NULL)
	{
		fprintf(stderr, "Could not allocate enough memory for data\n");
		exit(1);
	}
	srand((int)time(0));
	for(i = 0; i < N; i++)
	{
		float random = (dtype) rand() / (RAND_MAX);
		data[i] = (int)(DEFAULT_SCALE-1) * random + 1;
	}
	fwrite(&N, sizeof(int), 1, pFile);
	fwrite(&K, sizeof(int), 1, pFile);
	fwrite(data, sizeof(dtype), N, pFile);
	fclose(pFile);
	free(data);
}

void matrix_write(char *filename, int row, int column)
{
	int i, j;
	FILE *pFile = fopen(filename, "wb");
	if(pFile == NULL)
	{
		fputs("File error\n", stderr);
		exit(1);
	}

	dtype **matrix = (dtype **)malloc(row * PTR_SIZE);
	for(i = 0; i < row; i++)
	{
		if((*(matrix+i) = (dtype*)malloc(column * sizeof(dtype))) == NULL)
		{
			fprintf(stderr, "Could not allocate enough memory for matrix D\n");
			exit(1);
		}
	}

	srand((int)time(0));
	for(i = 0; i < row; i++)
	{
		for(j = 0; j < column; j++)
		{
			float random = (dtype) rand() / (RAND_MAX);
			matrix[i][j] = (int)(DEFAULT_SCALE-1) * random + 1;
		}
	}

	fwrite(&row, sizeof(int), 1, pFile);
	fwrite(&column, sizeof(int), 1, pFile);

	for(i=0; i<row; i++)
	{
		fwrite(matrix[i], sizeof(dtype), column, pFile);
	}
	fclose(pFile);

	for(i = 0; i < row; i++)
	{
		free(matrix[i]);
	}
	free(matrix);
}

