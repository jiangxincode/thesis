OBJECTS = ranksort k-means sat1 sat2 sat3 prime \
		  eratosthenes_parallel mat_vec_mul_v1 \
		  mat_gen mat_print matrix1 matrix2 
ALL: $(OBJECTS)

%: %.c
	mpicc -o $@ $^ -Wall -lm

mat_vec_mul_v1: mat_vec_mul_v1.c MyMPI.c
	mpicc -o $@ $^ -Wall

clean:
	rm $(OBJECTS)
