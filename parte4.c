#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <mpi.h>

#define NUM_THREADS 3

typedef enum _ActionType {
    event,
    receive,
    send,
    snapshot
} ActionType;

typedef struct _Action {
    int *clock;
    int target;
    ActionType type;
} Action;

typedef struct _Queue {
    int counter;
    Action *actions;
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
int read_input_action(Action *action) {
    char line[MAX_LINE_LENGTH], action[MAX_LINE_LENGTH];
    int action_rank, isEOF;
    do {
        fgets(line, MAX_LINE_LENGTH, input);
        isEOF = feof(input);
        sscanf(line, "%d %s", &action_rank);
    } while ((action_rank != rank || strcmp(action, "snapshot") == 0)
              && (!isEOF));
    if (!isEOF) {
        if (strcmp(action, "event") == 0) {
            action->type = event;
        }
        else if (strcmp(action, "snapshot") == 0) {
            action->type = snapshot;
            action->target = action_rank;
        }
        else if (strcmp(action, "send") == 0) {
            action->type = send;
            sscanf(line, "%*d send %d", &action->target);
        }
        else if (strcmp(action, "receive") == 0) {
            action->type = receive;
            sscanf(line, "%*d receive %d", &action->target);
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
    queue->actions = malloc(sizeof(Action) * queue_size);
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond_full, NULL);
    pthread_cond_init(&queue->cond_empty, NULL);
}

void enqueue(Queue* queue, Action action) {
    pthread_mutex_lock(&queue->mutex);
    if (queue->counter > queue_size) {
        pthread_cond_wait(&queue->cond_full, &queue->mutex);
    }
    queue->actions[queue->counter++] = action;
    if (queue->counter == 1) {
        // not empty anymore
        pthread_cond_signal(&queue->cond_empty);
    }
    pthread_mutex_unlock(&queue->mutex);
}

void shift_queue(Queue* queue) {
    for (size_t i = 1; i < queue->counter; i++) {
        queue->actions[i-1] = queue->actions[i];
    }
    queue->counter--;
}

void dequeue(Queue* queue, Action *action) {
    pthread_mutex_lock(&queue->mutex);
    if (queue->counter == 0) {
        pthread_cond_wait(&queue->cond_empty, &queue->mutex);
    }
    *action = queue->actions[0];
    shift_queue(queue);
    if (queue->counter == queue_size) {
        // not full anymore
        pthread_cond_signal(&queue->cond_full);
    }
    pthread_mutex_unlock(&queue->mutex);
}

void Send(Action* action, Queue *queue) {
    clock_event();
    copy_clock(global_clock, action->clock);
    enqueue(queue, *action);
}

void Receive(Action* action) {
    sync_clock(global_clock, action->clock);
    clock_event();
}

void *input_task(void *i) {
    Queue *queue = (Queue *)i;
    Action action;
    action.clock = malloc(sizeof(int) * comm_sz);
    while (read_input_action(&action) == SUCCESS) {
        if (action.type == receive) {
            MPI_Recv(action.clock, comm_sz, MPI_INT, 
                    action.target, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        enqueue(queue, action);
        action.clock = malloc(sizeof(int) * comm_sz);
    }
    return NULL;
}

void *clock_task(void *i) {
    Queue **queues = (Queue **)i;
    Action action;
    while (1) {
        dequeue(queues[0], &action);
        switch (action.type) {
            case event:
                clock_event();
                break;
            case send:
                Send(&action, queues[1]);
                break;
            case receive:
                Receive(&action);
                break;
        }
        print_clock(global_clock);
    }
}

void *output_task(void *i) {
    Queue *queue = (Queue *)i;
    Action action;
    while (1) {
        dequeue(queue, &action);
        MPI_Send(action.clock, comm_sz, MPI_INT, action.target, 0, MPI_COMM_WORLD);
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

