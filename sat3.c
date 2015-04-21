/*
 * Circuit Satisfiability, Version 3
 * 
 * This versio is for the calculating the execute time
 *
 */

#include <mpi.h>
#include <stdio.h>

/* Return 1 if 'i'th bit of 'n' is 1; 0 otherwise */
#define EXTRACT_BIT(n, i) ((n & (1<<i)) ? 1 : 0)

int check_circuit(int, int);

int main(int argc, char **argv)
{
    int global_solutions; //Total number of solutions
    int i;
    int id; // process rank
    int p; // number of processes
    int solutions; //Solutions found by this proc
    double elapsed_time;

    MPI_Init(&argc, &argv);
    MPI_Barrier(MPI_COMM_WORLD);
    elapsed_time = - MPI_Wtime();
    MPI_Comm_rank(MPI_COMM_WORLD, &id);
    MPI_Comm_size(MPI_COMM_WORLD, &p);

    solutions = 0;
    for(i=id; i<65536; i+=p)
    {
        solutions += check_circuit(id, i);
    }

    MPI_Reduce(&solutions, &global_solutions, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    elapsed_time += MPI_Wtime();

    printf("Process %d is done\n", id);
    fflush(stdout);
    MPI_Finalize();

    if(id == 0)
    {
        printf("There are %d different solutions\n", global_solutions);
        printf("This program cost %f ms\n", elapsed_time);

    }
    return 0;
}

int check_circuit(int id, int z)
{
    int v[16];
    int i;

    for(i=0; i<16; i++)
    {
        v[i]=EXTRACT_BIT(z, i);
    }

    if((v[0] || v[1])
            && (!v[1] || !v[3])
            && (v[2] || v[3])
            && (!v[3] || !v[4])
            && (v[4] || !v[5])
            && (v[5] || !v[6])
            && (v[5] || v[6])
            && (v[6] || !v[15])
            && (v[7] || !v[8])
            && (!v[7] || !v[13])
            && (v[8] || v[9])
            && (v[8] || !v[9])
            && (!v[9] || !v[10])
            && (v[9] || v[11])
            && (v[10] || v[11])
            && (v[12] || v[13])
            && (v[13] || !v[14])
            && (v[14] || v[15]))
    {
        return 1;
    }
    return 0;
}
