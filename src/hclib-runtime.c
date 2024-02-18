/*
 * Copyright 2017 Rice University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hclib-internal.h"
#include <stdbool.h>

pthread_key_t selfKey;
pthread_once_t selfKeyInitialized = PTHREAD_ONCE_INIT;

hclib_worker_state* workers;
int * worker_id;
int nb_workers;
int not_done;
static double user_specified_timer = 0;
static double benchmark_start_time_stats = 0;

// things added
bool* statuses;
int NO_REQUEST = -1;
int* request_cells;

int scratch_number = 69;
task_t scratch_value = {
        ._fp = NULL,
        .args = &scratch_number
};
task_t* NO_RESPONSE = &scratch_value;
task_t** transfer_cells;
void acquire(int i);



double mysecond() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + ((double) tv.tv_usec / 1000000);
}

// One global finish scope

static void initializeKey() {
    pthread_key_create(&selfKey, NULL);
}

void set_current_worker(int wid) {
    pthread_setspecific(selfKey, &workers[wid].id);
}

int hclib_current_worker() {
    return *((int *) pthread_getspecific(selfKey));
}

int hclib_num_workers() {
    return nb_workers;
}

//FWD declaration for pthread_create
void * worker_routine(void * args);

void setup() {
    // Build queues
    not_done = 1;
    pthread_once(&selfKeyInitialized, initializeKey);
    workers = (hclib_worker_state*) malloc(sizeof(hclib_worker_state) * nb_workers);

    // more random stuff
    statuses = (bool*) malloc(sizeof(bool) * nb_workers);
    request_cells = (int*) malloc(sizeof(int) * nb_workers);
    transfer_cells = malloc(sizeof(task_t*) * nb_workers);

    for(int i=0; i<nb_workers; i++) {
        transfer_cells[i] = (task_t*) malloc(sizeof(task_t));
      workers[i].deque = malloc(sizeof(deque_t));
      void * val = NULL;
      dequeInit(workers[i].deque, val);
      workers[i].current_finish = NULL;
      workers[i].id = i;
      statuses[i] = false;

      transfer_cells[i] = NO_RESPONSE;
    }
    // Start workers
    for(int i=1;i<nb_workers;i++) {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_create(&workers[i].tid, &attr, &worker_routine, &workers[i].id);
    }
    set_current_worker(0);
    // allocate root finish
    start_finish();
}

void check_in_finish(finish_t * finish) {
    if(finish) hc_atomic_inc(&(finish->counter));
}

void check_out_finish(finish_t * finish) {
    if(finish) hc_atomic_dec(&(finish->counter));
}

void hclib_init(int argc, char **argv) {
    printf("---------HCLIB_RUNTIME_INFO-----------\n");
    printf(">>> HCLIB_WORKERS\t= %s\n", getenv("HCLIB_WORKERS"));
    printf("----------------------------------------\n");
    nb_workers = (getenv("HCLIB_WORKERS") != NULL) ? atoi(getenv("HCLIB_WORKERS")) : 1;
    setup();
    benchmark_start_time_stats = mysecond();
}

void execute_task(task_t * task) {
    finish_t* current_finish = task->current_finish;
    int wid = hclib_current_worker();
    hclib_worker_state* ws = &workers[wid];
    ws->current_finish = current_finish;
    task->_fp((void *)task->args);
    check_out_finish(current_finish);
    free(task);
}

void spawn(task_t * task) {
    // get current worker
    int wid = hclib_current_worker();
    hclib_worker_state* ws = &workers[wid];
    check_in_finish(ws->current_finish);
    task->current_finish = ws->current_finish;
    // push on worker deq
    dequePush(ws->deque, task);
    ws->total_push++;
}

void hclib_async(generic_frame_ptr fct_ptr, void * arg) {
    task_t * task = malloc(sizeof(*task));
    *task = (task_t){
        ._fp = fct_ptr,
        .args = arg,
    };
    spawn(task);
}

void slave_worker_finishHelper_routine(finish_t* finish) {
   int wid = hclib_current_worker();
   while(finish->counter > 0) {
       task_t* task = dequePop(workers[wid].deque);
       if (!task) {
           // try to steal
           int i = 1;
           while(finish->counter > 0 && i < nb_workers) {
               task = dequeSteal(workers[(wid+i)%(nb_workers)].deque);
	       if(task) {
		   workers[wid].total_steals++;	   
	           break;
	       }
	       i++;
	   }
        }
        if(task) {
            execute_task(task);
        }
    }
}

void start_finish() {
    int wid = hclib_current_worker();
    hclib_worker_state* ws = &workers[wid];
    finish_t * finish = (finish_t*) malloc(sizeof(finish_t));
    finish->parent = ws->current_finish;
    check_in_finish(finish->parent);
    ws->current_finish = finish;
    finish->counter = 0;
}

void end_finish(){ 
    int wid = hclib_current_worker();
    hclib_worker_state* ws = &workers[wid];
    finish_t* current_finish = ws->current_finish;
    if (current_finish->counter > 0) {
        slave_worker_finishHelper_routine(current_finish);
    }
    assert(current_finish->counter == 0);
    check_out_finish(current_finish->parent); // NULL check in check_out_finish
    ws->current_finish = current_finish->parent;
    free(current_finish);
}

void hclib_finalize() {
    end_finish();
    not_done = 0;
    int i;
    int tpush=workers[0].total_push, tsteals=workers[0].total_steals;
    for(i=1;i< nb_workers; i++) {
        pthread_join(workers[i].tid, NULL);
	tpush+=workers[i].total_push;
	tsteals+=workers[i].total_steals;
    }
    double duration = (mysecond() - benchmark_start_time_stats) * 1000;
    printf("============================ Tabulate Statistics ============================\n");
    printf("time.kernel\ttotalAsync\ttotalSteals\n");
    printf("%.3f\t%d\t%d\n",user_specified_timer,tpush,tsteals);
    printf("=============================================================================\n");
    printf("===== Total Time in %.f msec =====\n", duration);
    printf("===== Test PASSED in 0.0 msec =====\n");

//    free(statuses);
//    free(request_cells);
//    for (int j = 0; j < nb_workers; ++j) {
//        free(transfer_cells[i]);
//    }
//    free(transfer_cells);
//    free(NO_RESPONSE);
}

void hclib_kernel(generic_frame_ptr fct_ptr, void * arg) {
    double start = mysecond();
    fct_ptr(arg);
    user_specified_timer = (mysecond() - start)*1000;
}

void hclib_finish(generic_frame_ptr fct_ptr, void * arg) {
    start_finish();
    fct_ptr(arg);
    end_finish();
}

void* worker_routine(void * args) {
    int wid = *((int *) args);
   set_current_worker(wid);
   while(not_done) {
       task_t* task = dequePop(workers[wid].deque);
       if (!task) {
           // try to steal
           int i = 1;
           while (i < nb_workers) {
               acquire(wid);
               task = dequePop(workers[wid].deque);
               if(task) {
                   workers[wid].total_steals++;
                   break;
               }
               i++;
           }
        }
       if(task) {
           workers[wid].total_steals++;
           execute_task(task);
       }
   }
   return NULL;
}

int has_no_work(int i) {
    return workers[i].deque->head == workers[i].deque->tail;
}
void update_status(int i) {
    int update = has_no_work(i);
    if (statuses[i] != update)
        statuses[i] = update;
}
void communicate(int i) {
    int thief = request_cells[i];
    if (thief == NO_REQUEST)
        return;

    if (has_no_work(i)) {
        transfer_cells[thief] = NULL;
    } else {
        transfer_cells[i] = dequeSteal(workers[i].deque);
    }
    request_cells[i] = NO_REQUEST;
}
void add_task(int i, task_t* task) {
    dequePush(workers[i].deque, task);
    update_status(i);
}
void acquire(int i) {
    while (true) {
        transfer_cells[i] = NO_RESPONSE;
        int k = (rand() % nb_workers) - 1;
        while (k == i)
            k = (rand() % nb_workers) - 1;

        if (statuses[k] && hc_cas(&request_cells[k], NO_REQUEST, i)) {
            while (transfer_cells[i] == NO_RESPONSE)
                communicate(i);
            if (transfer_cells[i] != NULL) {
                add_task(i, transfer_cells[i]);
                request_cells[i] = NO_REQUEST;
                return;
            }
        }
        communicate(i);
    }
}