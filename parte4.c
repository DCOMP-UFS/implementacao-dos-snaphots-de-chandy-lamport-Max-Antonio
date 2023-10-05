#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <mpi.h>

#define NOT_RECORDING (-1)
#define NUM_THREADS 3
#define MARKER_VAL (-1)

//////////////////////////////////////////////////
// Types
//////////////////////////////////////////////////

typedef enum _ActionType {
    event,
    receive,
    send,
    snapshot, // inicializar snapshot
    marker
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

//////////////////////////////////////////////////
// Globals and other utilities
//////////////////////////////////////////////////

int rank, comm_sz, *global_clock;
int has_seen_marker;
pthread_mutex_t seen_lock = PTHREAD_MUTEX_INITIALIZER;
int *channels;
pthread_mutex_t channels_lock = PTHREAD_MUTEX_INITIALIZER;
Queue *input_queue, *output_queue, *marker_queue;
int queue_size, num_actions;
FILE *input;

#define SUCCESS 1
#define FAILURE 0
#define MAX_LINE_LENGTH 100
int read_input_action(Action *action_p) {
    char line[MAX_LINE_LENGTH], action[MAX_LINE_LENGTH];
    int action_rank, isEOF;
    do {
        fgets(line, MAX_LINE_LENGTH, input);
        isEOF = feof(input);
        sscanf(line, "%d %s", &action_rank);
    } while (action_rank != rank && (!isEOF));
    if (!isEOF) {
        if (strcmp(action, "event") == 0) {
            action_p->type = event;
        }
        else if (strcmp(action, "snapshot") == 0) {
            action_p->type = snapshot;
        }
        else if (strcmp(action, "send") == 0) {
            action_p->type = send;
            sscanf(line, "%*d send %d", &action_p->target);
        }
        else if (strcmp(action, "receive") == 0) {
            action_p->type = receive;
            sscanf(line, "%*d receive %d", &action_p->target);
        }
        return SUCCESS;
    }
    return FAILURE;
}

//////////////////////////////////////////////////
// Clock operations
//////////////////////////////////////////////////

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

int is_marker(int *clock) {
    return clock[0] == MARKER_VAL;
}

//////////////////////////////////////////////////
// Queue operations
//////////////////////////////////////////////////

void init_queue(Queue* queue) {
    queue->actions = malloc(sizeof(Action) * queue_size);
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond_full, NULL);
    pthread_cond_init(&queue->cond_empty, NULL);
}

int is_empty(Queue *queue) {
    pthread_mutex_lock(&queue->mutex);
    int res = queue->counter == 0;
    pthread_mutex_unlock(&queue->mutex);
    return res;
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

//////////////////////////////////////////////////
// Snapshot
//////////////////////////////////////////////////

int check_marker_seen(void) {
    pthread_mutex_lock(&seen_lock);
    int res = has_seen_marker;
    pthread_mutex_unlock(&seen_lock);
    return res;
}

void make_marker_seen(void) {
    pthread_mutex_lock(&seen_lock);
    has_seen_marker = 1;
    pthread_mutex_unlock(&seen_lock);
}

void record_state(void) {
    printf("State of ");
    print_clock(global_clock);
}

void close_channel(int i, char *label) {
    pthread_mutex_lock(&channels_lock);
    channels[i] = NOT_RECORDING;
    printf("Channel C%d,%d: %s\n", i, rank, label);
    pthread_mutex_unlock(&channels_lock);
}

void emit_marker(void) {
    Action action;
    action.type = marker;
    action.clock = malloc(sizeof(int) * comm_sz);
    action.clock[0] = MARKER_VAL;
    action.clock[1] = rank;
    enqueue(output_queue, action);
}

int get_marker_sender(int *clock) {
    return clock[1];
}

void first_marker(int sender) {
    record_state();
    close_channel(sender, "EMPTY");
    emit_marker();
    make_marker_seen();
}

void another_marker(int sender) {
    close_channel(sender, "FINALIZED");
}

void start_snapshot() {
    record_state();
    emit_marker();
}

//////////////////////////////////////////////////
// Threads
//////////////////////////////////////////////////

void *input_task(void *i) {
    Action action;
    Queue *q;
    action.clock = malloc(sizeof(int) * comm_sz);
    while (1) {
        MPI_Recv(action.clock, comm_sz, MPI_INT,
                MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        q = is_marker(action.clock) ? marker_queue : input_queue;
        enqueue(q, action);
        action.clock = malloc(sizeof(int) * comm_sz);
    }
    return NULL;
}

void Send(Action* action) {
    action->clock = malloc(sizeof(int) * comm_sz);
    clock_event();
    copy_clock(global_clock, action->clock);
    enqueue(output_queue, *action);
}

void Receive(Action* action) {
    dequeue(input_queue, action);
    sync_clock(global_clock, action->clock);
    clock_event();
}

void *clock_task(void *i) {
    Action action;
    while (1) {
        if (!is_empty(marker_queue)) {
            dequeue(marker_queue, &action);
            int sender = get_marker_sender(action.clock);
            int marker_seen = check_marker_seen();
            if (marker_seen) {
                another_marker(sender);
            }
            else {
                first_marker(sender);
            }
        }
        if (read_input_action(&action) == SUCCESS) {
            switch (action.type) {
                case event:
                    clock_event();
                    break;
                case send:
                    Send(&action);
                    break;
                case receive:
                    Receive(&action);
                    break;
                case snapshot:
                    start_snapshot();
                    break;
            }
            print_clock(global_clock);
        }
    }
}

void *output_task(void *i) {
    Action action;
    while (1) {
        dequeue(output_queue, &action);
        switch (action.type) {
            case receive:
                MPI_Send(action.clock, comm_sz, MPI_INT,
                        action.target, 0, MPI_COMM_WORLD);
                break;
            case marker:
                MPI_Bcast(action.clock, comm_sz, MPI_INT, rank, MPI_COMM_WORLD);
                break;
        }
    }
}

//////////////////////////////////////////////////
// Main
//////////////////////////////////////////////////

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
    init_queue(input_queue);
    init_queue(output_queue);
    init_queue(marker_queue);
    // threads startup
    pthread_t threads[NUM_THREADS];
    pthread_create(&threads[0], NULL, input_task, NULL);
    pthread_create(&threads[1], NULL, clock_task, NULL);
    pthread_create(&threads[2], NULL, output_task,NULL);
    for (size_t i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    MPI_Finalize();
}

