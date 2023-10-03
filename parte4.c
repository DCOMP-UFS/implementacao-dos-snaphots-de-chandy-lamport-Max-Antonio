#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <mpi.h>

#define NUM_THREADS 3

typedef enum _MessageType {
    event,
    receive,
    send
} MessageType;

typedef struct _Message {
    int *clock;
    int target;
    MessageType type;
} Message;

typedef struct _Queue {
    int counter;
    Message *msgs;
    pthread_mutex_t mutex;
    pthread_cond_t cond_full;
    pthread_cond_t cond_empty;
} Queue;

// global process info
int rank, comm_sz, *global_clock;
Queue input_queue, output_queue;
int queue_size, num_actions;
FILE *input;

#define SUCCESS 1
#define FAILURE 0
#define MAX_LINE_LENGTH 100
int read_input_action(Message *msg) {
    char line[MAX_LINE_LENGTH], action[MAX_LINE_LENGTH];
    int action_rank, isEOF;
    do {
        fgets(line, MAX_LINE_LENGTH, input);
        isEOF = feof(input);
        sscanf(line, "%d", &action_rank);
        sscanf(line, "%*d %s", action);
    } while (action_rank != rank && (!isEOF));
    if (!isEOF) {
        if (strcmp(action, "event") == 0) {
            msg->type = event;
        }
        else if (strcmp(action, "send") == 0) {
            msg->type = send;
            sscanf(line, "%*d send %d", &msg->target);
        }
        else if (strcmp(action, "receive") == 0) {
            msg->type = receive;
            sscanf(line, "%*d receive %d", &msg->target);
        }
        return SUCCESS;
    }
    return FAILURE;
}

void clock_event(void) {
    global_clock[rank]++;
}

#define max(a, b) a > b ? a : b
void sync_clock(int *synced_clock, int *received_clock) {
    for (size_t i = 0; i < comm_sz; i++) {
        synced_clock[i] = max(synced_clock[i], received_clock[i]);
    }
}

void copy_clock(int *clock, int *copy) {
    for (size_t i = 0; i < comm_sz; i++) {
        copy[i] = clock[i];
    }
}

void print_clock(int *clock) {
    printf("process %d: (%d, %d, %d)\n", rank, clock[0], clock[1], clock[2]);
}

void init_queue(Queue* queue) {
    queue->msgs = malloc(sizeof(Message) * queue_size);
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond_full, NULL);
    pthread_cond_init(&queue->cond_empty, NULL);
}

void enqueue(Queue* queue, Message msg) {
    pthread_mutex_lock(&queue->mutex);
    if (queue->counter > queue_size) {
        pthread_cond_wait(&queue->cond_full, &queue->mutex);
    }
    queue->msgs[queue->counter++] = msg;
    if (queue->counter == 1) {
        // not empty anymore
        pthread_cond_signal(&queue->cond_empty);
    }
    pthread_mutex_unlock(&queue->mutex);
}

void shift_queue(Queue* queue) {
    for (size_t i = 1; i < queue->counter; i++) {
        queue->msgs[i-1] = queue->msgs[i];
    }
    queue->counter--;
}

void dequeue(Queue* queue, Message *msg) {
    pthread_mutex_lock(&queue->mutex);
    if (queue->counter == 0) {
        pthread_cond_wait(&queue->cond_empty, &queue->mutex);
    }
    *msg = queue->msgs[0];
    shift_queue(queue);
    if (queue->counter == queue_size) {
        // not full anymore
        pthread_cond_signal(&queue->cond_full);
    }
    pthread_mutex_unlock(&queue->mutex);
}

void Send(Message* msg, void *i) {
    Queue **queues = (Queue **)i;
    Queue *output_q = queues[1];
    clock_event();
    copy_clock(global_clock, msg->clock);
    enqueue(output_q, *msg);
}

void Receive(Message* msg, void *i) {
    Queue **queues = (Queue **)i;
    Queue *input_q = queues[0];
    sync_clock(global_clock, msg->clock);
    clock_event();
}

void *input_task(void *i) {
    Queue *queue = (Queue *)i;
    Message msg;
    msg.clock = malloc(sizeof(int) * comm_sz);
    while (read_input_action(&msg) == SUCCESS) {
        if (msg.type == receive) {
            MPI_Recv(msg.clock, comm_sz, MPI_INT, 
                    msg.target, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        enqueue(queue, msg);
        msg.clock = malloc(sizeof(int) * comm_sz);
    }
    return NULL;
}

void *clock_task(void *i) {
    Queue **queues = (Queue **)i;
    Queue *input_q = queues[0];
    Message msg;
    while (1) {
        dequeue(input_q, &msg);
        switch (msg.type) {
            case event:
                clock_event();
                break;
            case send:
                Send(&msg, i);
                break;
            case receive:
                Receive(&msg, i);
                break;
        }
        print_clock(global_clock);
    }
}

void *output_task(void *i) {
    Queue *queue = (Queue *)i;
    Message msg;
    while (1) {
        dequeue(queue, &msg);
        MPI_Send(msg.clock, comm_sz, MPI_INT, msg.target, 0, MPI_COMM_WORLD);
    }
}

int main(int argc, char **argv) {
    // IO startup
    input = fopen(argv[1], "r");
    if (!input) {
        fprintf(stderr, "usage: %s <actions file>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    // MPI startup
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    // globals startup
    fscanf(input, "%d\n", &queue_size);
    global_clock = calloc(comm_sz, sizeof(int));
    init_queue(&input_queue);
    init_queue(&output_queue);
    // threads startup
    pthread_t threads[NUM_THREADS];
    Queue* queues[2] = {&input_queue, &output_queue};
    pthread_create(&threads[0], NULL, input_task, (void *)&input_queue);
    pthread_create(&threads[1], NULL, clock_task, (void *)queues);
    pthread_create(&threads[2], NULL, output_task, (void *)&output_queue);
    for (size_t i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    MPI_Finalize();
}

