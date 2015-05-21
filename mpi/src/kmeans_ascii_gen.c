#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>

#define DEFAULT_LINE 1000
#define DEFAULT_DIMENSION 2

int main(int argc, char *argv[])
{
	int line = DEFAULT_LINE;
	int dimension = DEFAULT_DIMENSION;
	int isShowLineNum = 1;
	char separator = ' ';
	char *filename = NULL;
	int i, j;
	FILE *pFile;

	opterr = 0;
	int opt;
	while((opt = getopt(argc, argv, "l:d:f:s:m:h")) != EOF)
	{
		switch(opt)
		{
		case 'l':
			line = atoi(optarg);
			break;
		case 'd':
			dimension = atoi(optarg);
			break;
		case 'f':
			filename = optarg;
			break;
		case 's':
            isShowLineNum = atoi(optarg);
            break;
        case 'm':
            separator = optarg[0];
            break;
        case 'h':
            puts("Usage:");
            puts("-l line number");
            puts("-d dimension");
            puts("-f filename");
            puts("-s [0|1] write the line number or not");
            puts("-m set the separator");
            puts("-h show the help");
            break;
		case '?':
			fprintf(stderr, "Unknown option '-%c'\n", optopt);
			exit(1);
		}
	}

	pFile = fopen(filename, "w+");
	if(pFile == NULL)
	{
		fputs("file open error\n", stderr);
		exit(1);
	}

    srand(1);
	for(i=0; i<line; i++)
    {
        if(isShowLineNum)
        {
            fprintf(pFile, "%d%c", i+1, separator);
        }
        for(j=0; j<dimension-1; j++)
        {
            double random = (double)rand() / RAND_MAX;
            fprintf(pFile, "%.6f%c", 9*random+1, separator);
        }
        double random = (double)rand() / RAND_MAX;
        fprintf(pFile, "%.6f", 9*random+1);
        fprintf(pFile, "\n");
    }
    fclose(pFile);
    return 0;
}
