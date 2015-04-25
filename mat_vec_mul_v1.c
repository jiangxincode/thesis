/*
 * Matrix-vector multiplication, Version 1
 */

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include "MyMPI.h"

int main(int argc, char **argv)
{
    dtype **a; //first factor, a matrix
    dtype *b; //second factor, a vector
    dtype *c_block; //partial product vector
    dtype *c; //replicated product vector
    dtype *storage; //matrix elements stored here
    int i,j;
    int id; //process id number
    int p; //number of processes
    int m,n; //rows and columns in matrix
    int nprime; //elements in vector
    int rows; //number of rows of this process

    double elapsed_time;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &id);
    MPI_Comm_size(MPI_COMM_WORLD, &p);

    read_row_striped_matrix(argv[1], (void*)&a, (void*)&storage, mpitype, &m ,&n, MPI_COMM_WORLD);
    rows = BLOCK_SIZE(id, p, m);
    //print_row_striped_matrix((void**)a, mpitype, m, n, MPI_COMM_WORLD);
    read_replicated_vector(argv[2], (void*)&b, mpitype, &nprime, MPI_COMM_WORLD);
    //print_replicated_vector(b, mpitype, nprime, MPI_COMM_WORLD);
    
    MPI_Barrier(MPI_COMM_WORLD);
    elapsed_time = - MPI_Wtime();

    c_block = (dtype*)malloc(rows*sizeof(dtype));
    c = (dtype*)malloc(n*sizeof(dtype));
    for(i=0; i<rows; i++)
    {
        c_block[i] = 0.0;
        for(j=0; j<n; j++)
        {
            c_block[i] += a[i][j]*b[j];
        }
    }
    replicate_block_vector(c_block, n, (void*)c, mpitype, MPI_COMM_WORLD);
    //print_replicated_vector(c, mpitype, n, MPI_COMM_WORLD);

    //write_replicated_vector(c, mpitype, n, MPI_COMM_WORLD);

    elapsed_time += MPI_Wtime();
    if(id == 0)
    {
        printf("This program cost %f ms\n", elapsed_time);
    }
    MPI_Finalize();

    return 0;
}
