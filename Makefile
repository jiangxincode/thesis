OBJECTS = ranksort matrix_multiplication sat1 sat2 sat3 prime \
		  eratosthenes_parallel matrix_vector_multiplication_v1 
ALL: $(OBJECTS)

%: %.c
	mpicc -o $@ $^ -Wall -lm

matrix_vector_multiplication_v1: matrix_vector_multiplication_v1.c MyMPI.c
	mpicc -o $@ $^ -Wall

clean:
	rm $(OBJECTS)
