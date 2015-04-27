#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#define ROW_A 62
#define COLUMN_A 15
#define COLUMN_B 7
#define MASTER 0
#define FROM_MASTER 1
#define FROM_WORKER 2

MPI_Status status;

int main(int argc,char **argv)
{
    int p,                   //进程总数
        id,                        //进程标识
        numworkers,              //从进程数目
        rows,
        row_each,extra,offset,
        i,j,k;
    double start,end;
    double a[ROW_A][COLUMN_A],b[COLUMN_A][COLUMN_B],c[ROW_A][COLUMN_B];
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&id);
    MPI_Comm_size(MPI_COMM_WORLD,&p);
    numworkers = p-1; //从进程数目

    if(id==MASTER)
    {
        printf("Number of worker tasks=%d\n",numworkers);
        for(i=0; i<ROW_A; i++)
        {
            for(j=0; j<COLUMN_A; j++)
            {
                a[i][j]=((double)rand())/RAND_MAX;
            }
        }

        for(i=0; i<COLUMN_A; i++)
        {
            for(j=0; j<COLUMN_B; j++)
            {
                b[i][j]=((double)rand())/RAND_MAX;
            }
        }

        start=MPI_Wtime();
        row_each=ROW_A/numworkers;
        extra=ROW_A%numworkers;
        offset=0;

        for(i=1; i<=numworkers; i++)
        {
            rows=(i<=extra)?row_each+1:row_each;
            printf("sending %d rows to task %d\n",rows,i);
            MPI_Send(&offset,1,MPI_INT,i,FROM_MASTER,MPI_COMM_WORLD);
            MPI_Send(&rows,1,MPI_INT,i,FROM_MASTER,MPI_COMM_WORLD);
            MPI_Send(&a[offset][0],rows*COLUMN_A,MPI_DOUBLE,i,FROM_MASTER,MPI_COMM_WORLD);
            MPI_Send(&b,COLUMN_A*COLUMN_B,MPI_DOUBLE,i,FROM_MASTER,MPI_COMM_WORLD);
            offset=offset+rows;
        }

        for(i=1; i<=numworkers; i++)
        {
            MPI_Recv(&offset,1,MPI_INT,i,FROM_WORKER,MPI_COMM_WORLD,&status);
            MPI_Recv(&rows,1,MPI_INT,i,FROM_WORKER,MPI_COMM_WORLD,&status);
            MPI_Recv(&c[offset][0],rows*COLUMN_B,MPI_DOUBLE,i,FROM_WORKER,MPI_COMM_WORLD,&status);
        }

        end=MPI_Wtime();
        printf("Here is the result matrix\n");

        for(i=0; i<ROW_A; i++)
        {
            for(j=0; j<COLUMN_B; j++)
            {
                printf("%6.2f\t",c[i][j]);
            }
            printf("\n");
        }
        printf("the time is%lf\n",end-start);
    }

    if(id>MASTER)
    {
        printf("Master=%d,FROM_MASTER=%d\n",MASTER,FROM_MASTER);
        MPI_Recv(&offset,1,MPI_INT,MASTER,FROM_MASTER,MPI_COMM_WORLD,&status);
        printf("offset=%d\n",offset);
        MPI_Recv(&rows,1,MPI_INT,MASTER,FROM_MASTER,MPI_COMM_WORLD,&status);
        printf("row=%d\n",rows);
        MPI_Recv(&a,rows*COLUMN_A,MPI_DOUBLE,MASTER,FROM_MASTER,MPI_COMM_WORLD,&status);
        MPI_Recv(&b,COLUMN_A*COLUMN_B,MPI_DOUBLE,MASTER,FROM_MASTER,MPI_COMM_WORLD,&status);

        for(k=0; k<COLUMN_B; k++)
        {
            for(i=0; i<rows; i++)
            {
                c[i][k]=0.0;

                for(j=0; j<COLUMN_A; j++)
                {
                    c[i][k]=c[i][k]+a[i][j]*b[j][k];
                }
            }
        }

        MPI_Send(&offset,1,MPI_INT,MASTER,FROM_WORKER,MPI_COMM_WORLD);
        MPI_Send(&rows,1,MPI_INT,MASTER,FROM_WORKER,MPI_COMM_WORLD);
        MPI_Send(&c,rows*COLUMN_B,MPI_DOUBLE,MASTER,FROM_WORKER,MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
