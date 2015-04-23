#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include "MyMPI.h"

void matrix_write(char *filename, int row, int column, dtype **matrix);

int main(int argc, char *argv[])
{
    int row = DEFAULT_ROW;
    int column = DEFAULT_COLUMN;
    int scale = DEFAULT_SCALE;
    char *filename = DEFAULT_FILENAME;

    int i, j;
    dtype **matrix;

    opterr = 0;

    if (argc <= 1)
    {
        printf("no input arguments, using default value\n");
        printf("default matrix row: %d\n", row);
        printf("default matrix column: %d\n", column);
        printf("default filename: %s\n", filename);
        printf("defaule scale: %d\n", scale);
    }
    else
    {
        int opt;
        while((opt = getopt(argc, argv, "r:c:s:f:")) != EOF)
        {
            switch (opt)
            {
            case 'r':
                row = atoi(optarg);
                break;
            case 'c':
                column = atoi(optarg);
                break;
            case 's':
                scale = atoi(optarg);
                break;
            case 'f':
                filename = optarg;
                break;
            case '?':
                fprintf(stderr, "Unknown option '-%c'.\n", optopt);
                exit(1);
            default:
                fprintf(stderr, "Unknown option\n");
                exit(1);
            }
        }
    }

    matrix = (dtype **)malloc(row * PTR_SIZE);
    for (i = 0; i < row; i++)
    {
        if ((*(matrix+i) = (dtype*)malloc(column * sizeof(dtype))) == NULL)
        {
            fprintf(stderr, "Could not allocate enough memory for matrix D\n");
            exit(1);
        }
    }

    int seed = (int)time(0);
    float random;

    printf("@seed %d\n", seed);
    srand(seed);

    for (i = 0; i < row; i++)
    {
        for (j = 0; j < column; j++)
        {
            random = (dtype) rand() / (RAND_MAX);
            matrix[i][j] = (int) (scale-1) * random + 1;
        }
    }

    matrix_write(filename, row, column, matrix);

    for (i = 0; i < row; i++)
    {
        free(matrix[i]);
    }
    free(matrix);

    return 0;
}

void matrix_write(char *filename, int row, int column, dtype **matrix)
{
    FILE *pFile;

    pFile = fopen(filename, "wb");
    if (pFile == NULL)
    {
        fputs("File error\n", stderr);
        exit(1);
    }

    fwrite(&row, sizeof(int), 1, pFile);
    fwrite(&column, sizeof(int), 1, pFile);

    int i;
    for(i=0;i<row;i++)
    {
        fwrite(matrix[i], sizeof(dtype), column, pFile);
    }
    fclose(pFile);
}

