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

pthread_key_t selfKey;
pthread_once_t selfKeyInitialized = PTHREAD_ONCE_INIT;

hclib_worker_state* workers;
int * worker_id;
int nb_workers;
int not_done;
static double user_specified_timer = 0;
static double benchmark_start_time_stats = 0;

// added things
int nb_domains;
int nb_superdomains;
int n_workers;

void hws_steal_within_domain(int w_domain, int wid, hclib_worker_state* worker) {
    // if you know worker domain, wid you can figure out the other workers
    for (int i = w_domain * n_workers; i < (w_domain + 1) * n_workers; i++)
    {
        if (i == wid)
        continue;
        worker->hws_object->hierarchy[worker->hws_object->iter++] = i;
        //printf("%d ", i);
    }
}
void hws_steal_within_superdomain(int w_sd, int w_domain, int wid, hclib_worker_state* worker) {
    // if you know the worker's sd, you can find other domains
    for(int i = w_sd * nb_domains; i < (w_sd + 1) * nb_domains; i++) {
        if (i == w_domain)
        continue;
        hws_steal_within_domain(i, wid, worker);
    }
}
void hws_steal_within_other_sd(int w_sd, int w_domain, int wid, hclib_worker_state* worker) {
    for (int i = 0; i < nb_superdomains; i++) {
        if (i == w_sd)
        continue;
        hws_steal_within_superdomain(i, w_domain, wid, worker);
    }
}


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
    for(int i=0; i<nb_workers; i++) {
      workers[i].deque = malloc(sizeof(deque_t));
      void * val = NULL;
      dequeInit(workers[i].deque, val);
      workers[i].current_finish = NULL;
      workers[i].id = i;

      workers[i].hws_object = (hws*) malloc(sizeof(hws));
      workers[i].hws_object->iter = 1;
      workers[i].hws_object->hierarchy = (int*)malloc(sizeof(int) * nb_workers);
      workers[i].hws_object->hierarchy[0] = i;
    }

    // assign stealing affinities
    for (int i = 0; i < nb_superdomains; i++) {
        for (int j = i * nb_domains; j < (i + 1) * nb_domains; j++){
            // now select a worker to set hws
            // can you get the range of workers using only
            // nb_superdomains, nb_domains?
            for (int k = j * n_workers; k < (j + 1) * n_workers; k++){
                hws_steal_within_domain(j, k, &workers[k]);
                hws_steal_within_superdomain(i, j, k, &workers[k]);
                hws_steal_within_other_sd(i, j, k, &workers[k]);
            }   
        } 
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
    printf(">>> HCLIB_DOMAINS\t= %s\n", getenv("HCLIB_DOMAINS"));
    printf(">>> HCLIB_S_DOMAINS\t= %s\n", getenv("HCLIB_S_DOMAINS"));
    printf("----------------------------------------\n");
    n_workers = (getenv("HCLIB_WORKERS") != NULL) ? atoi(getenv("HCLIB_WORKERS")) : 1;
    nb_domains = (getenv("HCLIB_DOMAINS") != NULL) ? atoi(getenv("HCLIB_DOMAINS")) : 1;
    nb_superdomains = (getenv("HCLIB_S_DOMAINS") != NULL) ? atoi(getenv("HCLIB_S_DOMAINS")) : 1;
    nb_workers = nb_superdomains * nb_domains * n_workers;
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
            for (int i = 1; i < nb_workers; i++){
                int index = workers[wid].hws_object->hierarchy[i];
                task = dequeSteal(workers[index].deque);
                if(task) {
                    workers[wid].total_steals++;
                    break;
                }
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
        //    int i = 1;
        //    while (i < nb_workers) {
        //        task = dequeSteal(workers[(wid+i)%(nb_workers)].deque);
	    //    if(task) {
        //            workers[wid].total_steals++;
        //            break;
        //        }
	    //    i++;
            for (int i = 1; i < nb_workers; i++){
                int index = workers[wid].hws_object->hierarchy[i];
                task = dequeSteal(workers[index].deque);
                if(task) {
                    workers[wid].total_steals++;
                    break;
                }
            }
	    }
        if(task) {
            // printf("lmao I never stole anything gg\n");
            execute_task(task);
        }
    }
    return NULL;
}
