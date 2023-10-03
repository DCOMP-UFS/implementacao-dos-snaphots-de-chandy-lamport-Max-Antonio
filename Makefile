CC = mpicc
CC_FLAGS = -g -Wall
LD_FLAGS = -lpthread -lrt

SOURCE = $(wildcard *.c)
BINARIES = $(patsubst %.c,%,$(SOURCE))

all: $(BINARIES)

%: %.c
	$(CC) $(CC_FLAGS) $(LD_FLAGS) $^ -o $@

run: parte4
	mpiexec --oversubscribe -n 3 $^ $^.input

clean:
	rm -rf $(BINARIES)
