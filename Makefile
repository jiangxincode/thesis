OBJECTS = ranksort matrix_multiplication sat1
ALL: $(OBJECTS)

%: %.c
	mpicc -o $@ $^
clean:
	rm $(OBJECTS)
