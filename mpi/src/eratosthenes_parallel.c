#include <mpi.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>

#define BLOCK_LOW(id,p,n)  ((id)*(n)/(p))
#define BLOCK_HIGH(id,p,n) (BLOCK_LOW((id)+1,p,n)-1)

int main(int argc, char **argv)
{
	int count; //local prime count
	double elapse_time; //parallel execution time
	int first; //index of first multiple
	int global_count; //global prime count
	int high_value; //highest value on this proc
	int i;
	int id; //process ID number
	int index; //index of current prime
	int low_value; //lowest value on this proc
	char *marked; //portion of 2,...,'n'
	int n;
	int p; //number of processed
	int proc0_size; //size of the 0's sbuarray
	int prime; //current prime
	int size; //elements in 'marked'

	MPI_Init(&argc, &argv);

	MPI_Barrier(MPI_COMM_WORLD);
	elapse_time = - MPI_Wtime();
	MPI_Comm_rank(MPI_COMM_WORLD, &id);
	MPI_Comm_size(MPI_COMM_WORLD, &p);

	if(argc != 2)
	{
		if(!id)
		{
			printf("Command line: %s <m>\n", argv[0]);
		}
		MPI_Finalize();
		exit(1);
	}
	n = atoi(argv[1]);

	low_value = 2 + BLOCK_LOW(id, p, n-1);
	high_value = 2 + BLOCK_HIGH(id, p, n-1);
	size = high_value-low_value+1;

	proc0_size = (n-1)/p;
	if((2+proc0_size) < (int)sqrt((double)n))
	{
		if(!id)
		{
			printf("Too many processes\n");
		}
		MPI_Finalize();
		exit(1);
	}

	marked = (char*) malloc(size);
	if(marked == NULL)
	{
		printf("Cannot allocate enough memory\n");
		MPI_Finalize();
		exit(1);
	}

	for(i=0; i<size; i++)
	{
		marked[i]=0;
	}
	if(!id)
	{
		index = 0;
	}
	prime = 2;
	do
	{
		if(prime * prime > low_value)
		{
			first = prime * prime - low_value;
		}
		else
		{
			if(!(low_value%prime))
			{
				first = 0;
			}
			else
			{
				first = prime-(low_value%prime);
			}
		}
		for(i=first; i<size; i+=prime)
		{
			marked[i] = 1;
		}
		if(!id)
		{
			while(marked[++index]);
			prime = index+2;
		}
		MPI_Bcast(&prime, 1, MPI_INT, 0, MPI_COMM_WORLD);
	}
	while(prime*prime <= n);

	count = 0;
	for(i=0; i<size; i++)
	{
		if(!marked[i])
		{
			count++;
		}
	}
	MPI_Reduce(&count,&global_count,1,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD);

	elapse_time += MPI_Wtime();

	if(!id)
	{
		printf("%d primes are less than or equal to %d\n",global_count,n);
		printf("Total elapsed time: %10.6f\n", elapse_time);
	}

	MPI_Finalize();
	return 0;
}
