OBJECTS = ranksort matrix_multiplication sat1 sat2
ALL: $(OBJECTS)

%: %.c
	mpicc -o $@ $^
clean:
	rm $(OBJECTS)
