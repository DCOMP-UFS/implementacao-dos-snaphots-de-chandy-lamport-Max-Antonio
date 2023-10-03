CC = mpicc
CC_FLAGS = -g -Wall
LD_FLAGS = -lpthread -lrt

SOURCE = $(wildcard *.c)
BINARIES = $(patsubst %.c,%,$(SOURCE))

all: $(BINARIES)

%: %.c
	$(CC) $(CC_FLAGS)  $^ -o $@ $(LD_FLAGS)

run: parte4
	mpiexec -n 3 ./$^ $^.input

clean:
	rm -rf $(BINARIES)
