#include <stdio.h>
#include <stdlib.h>

#include "MyMPI.h"

int mat_print(char *filename, int *row, int *column)
{
    FILE *pFile;

    dtype** matrix;
    int i, j;

    pFile = fopen(filename, "rb");
    if (pFile == NULL)
    {
        fputs("File error\n", stderr);
        exit(1);
    }

    rewind(pFile);

    fread(row, sizeof(int), 1, pFile);
    fread(column, sizeof(int), 1, pFile);

    matrix = (dtype **)malloc(*row * PTR_SIZE);
    for(i=0; i<*row; i++)
    {
        *(matrix+i) = (dtype*)malloc(*column * sizeof(dtype));
    }
    for(i=0; i<(*row); i++)
    {
        fread(matrix[i], sizeof(dtype), (*column), pFile);
    }

    for (i = 0; i < *row; i++)
    {
        for (j = 0; j < *column; j++)
        {
            printf("%f ", matrix[i][j]);            
        }
        printf("\n");
    }

    fclose(pFile);
    free(matrix);

    return 0;
}


int main(int argc, char **argv)
{
    int row, column;
    char *filename = DEFAULT_FILENAME;
    if(argc != 2)
    {
        printf("no input arguments, using default value\n");
        printf("default filename: %s\n", filename);
    }
    else
    {
        filename = argv[1];
    }
    mat_print(filename, &row, &column);
    return 0;
}
