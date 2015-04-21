OBJECTS = ranksort matrix_multiplication sat1 sat2 sat3 prime \
		  eratosthenes_parallel
ALL: $(OBJECTS)

%: %.c
	mpicc -o $@ $^ -Wall -lm
clean:
	rm $(OBJECTS)
