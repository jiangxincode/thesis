#include <stdio.h>
#include  "mpi.h"

#define NRA 62                     //矩阵A的行数
#define NCA 15                     //矩阵A的列数
#define NCB 7                       //矩阵B的列数
#define MASTER 0               //主进程号
#define FROM_MASTER 1
#define FROM_WORKER 2

MPI_Status status;

main(int argc,char **argv)
{
    int numtasks,                   //进程总数
        taskid,                        //进程标识
        numworkers,              //从进程数目
        source,                      //消息源
        dest,                          //消息目的地
        nbytes,
        mtype,                       //消息类型
        intsize,
        dbsize,
        rows,
        averow,extra,offset,
        i,j,k,
        count;
    double star,end;
    double a[NRA][NCA],b[NCA][NCB],c[NRA][NCB];
    intsize = sizeof(int);
    dbsize = sizeof(double);
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&taskid);
    MPI_Comm_size(MPI_COMM_WORLD,&numtasks);
    numworkers = numtasks-1;     //从进程数目

    /* 程序采用主从模式，以下为主进程程序 */

    if(taskid==MASTER)
    {
        printf("Number of worker tasks=%d\n",numworkers);

        //矩阵赋值

        for(i=0; i<NRA; i++)
            for(j=0; j<NCA; j++)
            {
                a[i][j]=i+j;
            }

        for(i=0; i<NCA; i++)
            for(j=0; j<NCB; j++)
            {
                b[i][j]=1;
            }

        /*将数据发送到从进程*/
        star=MPI_Wtime();
        averow=NRA/numworkers;
        extra=NRA%numworkers;
        offset=0;
        mtype=FROM_MASTER;

        for(dest=1; dest<=numworkers; dest++)
        {
            rows=(dest<=extra)?  averow+1:averow;
            printf("sending %d rows to task %d\n",rows,dest);
            MPI_Send(&offset,1,MPI_INT,dest,mtype,MPI_COMM_WORLD);
            MPI_Send(&rows,1,MPI_INT,dest,mtype,MPI_COMM_WORLD);
            count=rows*NCA;
            MPI_Send(&a[offset][0],count,MPI_DOUBLE,dest,mtype,MPI_COMM_WORLD);
            count=NCA*NCB;
            MPI_Send(&b,count,MPI_DOUBLE,dest,mtype,MPI_COMM_WORLD);
            offset=offset+rows;
        }

        /*等待接收从进程计算结果*/
        mtype=FROM_WORKER;

        for(i=1; i<=numworkers; i++)
        {
            source=i;
            MPI_Recv(&offset,1,MPI_INT,source,mtype,MPI_COMM_WORLD,&status);
            MPI_Recv(&rows,1,MPI_INT,source,mtype,MPI_COMM_WORLD,&status);
            count=rows*NCB;
            MPI_Recv(&c[offset][0],count,MPI_DOUBLE,source,mtype,MPI_COMM_WORLD,&status);
        }

        /*主进程打印结果*/
        end=MPI_Wtime();
        printf("Here is the result matrix\n");

        for(i=0; i<NRA; i++)
        {
            printf("\n");

            for(j=0; j<NCB; j++)
            {
                printf("%6.2f    ",c[i][j]);
            }
        }

        printf("\n");
        printf("the time is%lf\n",end-star);
    }

    /* 以下为从进程*/

    if(taskid>MASTER)
    {
        mtype=FROM_MASTER;
        source=MASTER;
        printf("Master=%d,mtype=%d\n",source,mtype);
        MPI_Recv(&offset,1,MPI_INT,source,mtype,MPI_COMM_WORLD,&status);
        printf("offset=%d\n",offset);
        MPI_Recv(&rows,1,MPI_INT,source,mtype,MPI_COMM_WORLD,&status);
        printf("row=%d\n",rows);
        count=rows*NCA;
        MPI_Recv(&a,count,MPI_DOUBLE,source,mtype,MPI_COMM_WORLD,&status);
        printf("a[0][0]=%lf\n",a[0][0]);
        count=NCA*NCB;
        MPI_Recv(&b,count,MPI_DOUBLE,source,mtype,MPI_COMM_WORLD,&status);
        printf("b[0][0]=%lf\n",b[0][0]);

        for(k=0; k<NCB; k++)
            for(i=0; i<rows; i++)
            {
                c[i][k]=0.0;

                for(j=0; j<NCA; j++)
                {
                    c[i][k]=c[i][k]+a[i][j]*b[j][k];
                }
            }

        printf("c[0][1]=%lf\n",c[0][1]);
        mtype=FROM_WORKER;
        printf("after computer\n");
        MPI_Send(&offset,1,MPI_INT,MASTER,mtype,MPI_COMM_WORLD);
        MPI_Send(&rows,1,MPI_INT,MASTER,mtype,MPI_COMM_WORLD);
        MPI_Send(&c,rows*NCB,MPI_DOUBLE,MASTER,mtype,MPI_COMM_WORLD);
        printf("after send\n");
    }

    MPI_Finalize();
}
