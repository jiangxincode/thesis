#include <stdio.h>
#include <string.h>

#define N 100000


int main()
{
    int  flag[N];
    int  prime[20000];
    memset(flag,0,sizeof(flag));  //全部置为0
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
    for(i; i <= N; i++)  //从i统计到N便是求得的2——N内的素数
    {
        if(flag[i] == 0) prime[q++] = i;
    }
    for(i = 0; i < 25; i++) //打印前25个素数供你检查，就是100以内的那25个素数
    {
        printf("%d ",prime[i]);
    }
    printf("\n");
    return 0;
}
