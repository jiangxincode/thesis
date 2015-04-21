#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv)
{
    if(argc != 2)
    {
        printf("Command lines: %s <m>\n", argv[0]);
        exit(1);
    }
    int N = atoi(argv[1]);
    int  *flag = (int *)malloc(sizeof(int)*N);
    int  *prime = (int *)malloc(sizeof(int)*N);
    memset(flag,0,sizeof(int)*N);  //全部置为0

    int q = 0; //prime数组的下标
    int i, j;
    for(i = 2; i * i < N; i++)
    {
        if(flag[i])
        {
            continue;       //表示i为前面某个数的倍数，肯定不是素数
        }
        prime[q++] = i;
        for(j = i * i; j < N; j += i) //将是i倍数的全部筛掉
        {
            flag[j] = 1;
        }
    }
    for(; i <= N; i++)  //从i统计到N便是求得的2——N内的素数
    {
        if(flag[i] == 0) prime[q++] = i;
    }

    printf("%d\n", q);
    return 0;
}
