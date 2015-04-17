OBJECTS = ranksort
CC = mpicc
ranksort:ranksort.c
	$(CC) -o $@ $^
clean:
	rm $(OBJECTS)
