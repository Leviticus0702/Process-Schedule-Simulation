/**
 * @mainpage Process Simulation
 *
 */

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include "proc_structs.h"
#include "proc_syntax.h"
#include "logger.h"
#include "manager.h"

#define LOWEST_PRIORITY INT_MAX // 0 is highest, setting INT_kAX as lowest

static pcb_queue_t terminatedq;
static pcb_queue_t waitingq;
static pcb_queue_t readyq;
static resource_t *system_resources;
static int procTotal = 0;
static int procTerminated = 0;
static schedule_t schedAlgorithm;
static int threadCount;

bool_t terminate();
void schedule_fcfs();
void schedule_rr(int quantum);
void schedule_priority();
bool_t higher_priority(int, int);
struct pcb_t *detect_deadlock(void);

void execute_instr(pcb_t *proc);
void request_resource(pcb_t *proc);
void release_resource(pcb_t *proc);
bool_t acquire_resource(pcb_t *proc, char *resource_name);

void enqueue_pcb(pcb_t *proc, pcb_queue_t *queue, int status);
pcb_t *dequeue_pcb(pcb_queue_t *queue);

int get_num_threads(int num_args, char **argv);
char *get_data(int num_args, char **argv);
int get_algo(int num_args, char **argv);
int get_time_quantum(int num_args, char **argv);
void print_args(int num_thr, char *data, int sched, int tq);

int main(int argc, char** argv) {
    int num_thr = get_num_threads(argc, argv);
    threadCount = num_thr;
    char *data = get_data(argc, argv);
    int scheduler = get_algo(argc, argv);
    int time_quantum = get_time_quantum(argc, argv);
    print_args(num_thr, data, scheduler, time_quantum);
    bool_t success = FALSE;

    if (strcmp(data,"generate") == 0) {
        #ifdef DEBUG_MNGR
        printf("****Generate processes and initialise the system\n");
        #endif
        success = init_loader_from_generator();
    } else {
        #ifdef DEBUG_MNGR
        printf("Parse process file and initialise the system: %s \n", data);
        #endif
        success = init_loader_from_files(data);
    }

    if (success) {
        init_system();
        system_resources = get_resources();
        printf("***********Scheduling processes************\n");
        schedule_processes(num_thr, scheduler, time_quantum);
        dealloc_data_structures();
    } else {
        printf("Error: no processes to schedule\n");
    }

    return EXIT_SUCCESS;
}

/**
 * @brief The linked list of loaded processes is moved to the readyq.
 *    The waiting and terminated queues are intialised to empty
 */
void init_system(void)
{
    pcb_t *firstProc = longterm_scheduler();
        readyq.first = firstProc;
        readyq.last = NULL;
        for (pcb_t *iter = firstProc; iter; iter = iter->next) {
            iter->state = READY;
            procTotal++;
            readyq.last = iter;
        }
    procTerminated = 0;
    waitingq.last = NULL;
    waitingq.first = NULL;
    terminatedq.last = NULL;
    terminatedq.first = NULL;

    log_queue(readyq.first, "Ready");
    log_queue(waitingq.first, "Waiting");
    log_queue(terminatedq.first, "Terminated");
    log_msg("\n");

}

/** @brief Schedules each instruction of each process */
void schedule_processes(int num_thr, schedule_t sched_type, int quantum)
{
    schedAlgorithm = sched_type;
    switch (sched_type) {
        case PRIOR:
        schedule_priority();
        break;
        case RR:
        schedule_rr(quantum);
        break;
        case FCFS:
        schedule_fcfs();
        break;
        default:
        break;
    }
}

/** @brief Return true when there are no more processes to schedule */
bool_t terminate() {
    int waitCount = 0;
    for (pcb_t *p = waitingq.first; p; p = p->next)
        waitCount++;
    return (readyq.first == NULL && (procTerminated + waitCount) == procTotal) ? TRUE :FALSE;
}

/**
 * @brief Call the longterm schedule to check for new arrivals
 * If there are new arrivals, call
 *  log_pcbs("New arrivals in ready queue", new_arrivals);
 */
void load_new_processes(void) {
    pcb_t *newList = longterm_scheduler();
    if (newList) {
        log_pcbs("New arrivals in ready queue", newList);
        for (pcb_t *p = newList, *nextP; p; p = nextP) {
            nextP = p->next;
            enqueue_pcb(p, &readyq, READY);
            procTotal++;
        }
    }
}

/** Schedules processes using FCFS scheduling */
void schedule_fcfs(void) {
#pragma omp parallel num_threads(threadCount) shared(readyq, terminatedq, waitingq, procTerminated)
{
    while (1) {
        if (terminate()) break;
        
        pcb_t *victim = NULL;
        #pragma omp critical(deadlock_check)
        {
            victim = detect_deadlock();
        }
        if (victim != NULL) {
            #pragma omp critical(queue_mod)
            {
                pcb_t *prev = NULL;
                pcb_t *cur = waitingq.first;
                while (cur) {
                    if (cur == victim) {
                        if (prev)
                            prev->next = cur->next;
                        else
                            waitingq.first = cur->next;
                        if (cur == waitingq.last)
                            waitingq.last = prev;
                        break;
                    }
                    prev = cur;
                    cur = cur->next;
                }
                victim->state = TERMINATED;
                enqueue_pcb(victim, &terminatedq, TERMINATED);
                procTerminated++;
            }
        } else {
            pcb_t *proc = NULL;
            #pragma omp critical(ready_queue)
            {
                proc = dequeue_pcb(&readyq);
            }
            if (!proc)
                continue;
            proc->state = RUNNING;
            while (proc->next_instruction && proc->state == RUNNING) {
                execute_instr(proc);
                #pragma omp critical(new_load)
                {
                    load_new_processes();
                }
            }
            #pragma omp critical(queue_mod)
            {
                if (proc->state == RUNNING && !proc->next_instruction) {
                    proc->state = TERMINATED;
                    enqueue_pcb(proc, &terminatedq, TERMINATED);
                    procTerminated++;
                } else if (proc->state == READY)
                    enqueue_pcb(proc, &readyq, READY);
            }
        }
    }
}
}

/** Schedules processes using the Round-Robin scheduler. */
void schedule_rr(int quantum) {
    if (quantum <= 0)
    quantum = 1;
    #pragma omp parallel num_threads(threadCount) shared(readyq, terminatedq, waitingq, procTerminated)
    {
    while (1) {
        if (terminate()) break;
        
        pcb_t *victim = NULL;
        #pragma omp critical(deadlock_check)
        {
            victim = detect_deadlock();
        }
        if (victim != NULL) {
            #pragma omp critical(queue_mod)
            {
                pcb_t *prev = NULL;
                pcb_t *cur = waitingq.first;
                while (cur) {
                    if (cur == victim) {
                        if (prev)
                            prev->next = cur->next;
                        else
                            waitingq.first = cur->next;
                        if (cur == waitingq.last)
                            waitingq.last = prev;
                        break;
                    }
                    prev = cur;
                    cur = cur->next;
                }
                victim->state = TERMINATED;
                enqueue_pcb(victim, &terminatedq, TERMINATED);
                procTerminated++;
            }
        } else {
            pcb_t *proc = NULL;
            #pragma omp critical(ready_queue)
            {
                proc = dequeue_pcb(&readyq);
            }
            if (!proc)
                continue;
            proc->state = RUNNING;
            int ticks = 0;
            while (proc->state == RUNNING && ticks < quantum && proc->next_instruction) {
                execute_instr(proc);
                ticks++;
                #pragma omp critical(new_load)
                {
                    load_new_processes();
                }
            }
            #pragma omp critical(queue_mod)
            {
                if (proc->state == RUNNING) {
                    if (!proc->next_instruction) {
                        proc->state = TERMINATED;
                        enqueue_pcb(proc, &terminatedq, TERMINATED);
                        procTerminated++;
                    } else {
                        enqueue_pcb(proc, &readyq, READY);
                    }
                }
            }
        }
    }
    }
}

/** Schedules processes using priority scheduling with preemption */
void schedule_priority(void) {
    #pragma omp parallel num_threads(threadCount) shared(readyq, terminatedq, procTerminated)
    {
        while (1) {
            if (terminate()) break;
            
            pcb_t *victim = NULL;
            #pragma omp critical(deadlock_check)
            {
                victim = detect_deadlock();
            }
            if (victim != NULL) {
                #pragma omp critical(queue_mod)
                {
                    pcb_t *prev = NULL;
                    pcb_t *cur = waitingq.first;
                    while (cur) {
                        if (cur == victim) {
                            if (prev)
                                prev->next = cur->next;
                            else
                                waitingq.first = cur->next;
                            if (cur == waitingq.last)
                                waitingq.last = prev;
                            break;
                        }
                        prev = cur;
                        cur = cur->next;
                    }
                    victim->state = TERMINATED;
                    enqueue_pcb(victim, &terminatedq, TERMINATED);
                    procTerminated++;
                }
            } else {
                pcb_t *proc = NULL;
                #pragma omp critical(ready_queue)
                {
                    if (readyq.first) {
                        pcb_t *prev = NULL, *bestPrev = NULL;
                        proc = readyq.first;
                        for (pcb_t *cur = readyq.first; cur; cur = cur->next) {
                            if (higher_priority(cur->priority, proc->priority)) {
                                proc = cur;
                                bestPrev = prev;
                            }
                            prev = cur;
                        }
                        if (bestPrev)
                            bestPrev->next = proc->next;
                        else
                            readyq.first = proc->next;
                        if (proc == readyq.last)
                            readyq.last = bestPrev;
                        proc->next = NULL;
                    }
                }
                if (!proc)
                    continue;
                proc->state = RUNNING;
                while (proc->next_instruction && proc->state == RUNNING) {
                    execute_instr(proc);
                    #pragma omp critical(new_load)
                    {
                        load_new_processes();
                    }
                    if (proc->state == RUNNING) {
                        pcb_t *cand = NULL;
                        #pragma omp critical(ready_queue)
                        {
                            if (readyq.first) {
                                cand = readyq.first;
                                for (pcb_t *cur = readyq.first; cur; cur = cur->next) {
                                    if (higher_priority(cur->priority, cand->priority))
                                        cand = cur;
                                }
                            }
                        }
                        if (cand && higher_priority(cand->priority, proc->priority)) {
                            proc->state = READY;
                            #pragma omp critical(queue_mod)
                            {
                                enqueue_pcb(proc, &readyq, READY);
                            }
                            #pragma omp critical(ready_queue)
                            {
                                pcb_t *prev = NULL, *bestPrev = NULL;
                                cand = readyq.first;
                                for (pcb_t *cur = readyq.first; cur; cur = cur->next) {
                                    if (higher_priority(cur->priority, cand->priority)) {
                                        cand = cur;
                                        bestPrev = prev;
                                    }
                                    prev = cur;
                                }
                                if (bestPrev)
                                    bestPrev->next = cand->next;
                                else
                                    readyq.first = cand->next;
                                if (cand == readyq.last)
                                    readyq.last = bestPrev;
                                cand->next = NULL;
                                proc = cand;
                            }
                            proc->state = RUNNING;
                        }
                    }
                }
                #pragma omp critical(queue_mod)
                {
                    if (proc && proc->state == RUNNING && !proc->next_instruction) {
                        proc->state = TERMINATED;
                        enqueue_pcb(proc, &terminatedq, TERMINATED);
                        procTerminated++;
                    }
                }
            }
        }
    }
}

/** @brief Return TRUE if pr1 has a higher priority than pr2 */
bool_t higher_priority(int pr1, int pr2) {
    return (pr1 < pr2) ? TRUE : FALSE;
}

/**
 * @brief detect deadlock
 * If deadlock is detected, call
 *  log_deadlock_detected();
 */
struct pcb_t *detect_deadlock(void) {
    // For each process in the waitingq, traverse its wait-for chain.
    for (pcb_t *proc = waitingq.first; proc; proc = proc->next) {
        if (proc->state != WAITING)
            continue;
        int capacity = procTotal;
        pcb_t **visited = (pcb_t **)malloc(capacity * sizeof(pcb_t *));
        int count = 0;
        pcb_t *current = proc;
        int cycleFound = 0;
        while (current && current->state == WAITING && current->next_instruction) {
            int i, seen = 0;
            for (i = 0; i < count; i++) {
                if (visited[i] == current) { seen = 1; break; }
            }
            if (seen) { cycleFound = 1; break; }
            visited[count++] = current;
            char *needed = current->next_instruction->resource_name;
            if (!needed)
                break;
            resource_t *res = system_resources;
            while (res && strcasecmp(res->name, needed) != 0)
                res = res->next;
            if (!res || !res->allocated)
                break;
            current = res->allocated;
        }
        free(visited);
        if (cycleFound) {
            log_deadlock_detected();
            return proc;
        }
    }
    return NULL;
}

/** Call the correct function to execute the next instruction of the process
  *  If there is no instruction to execute, call:
  *   log_msg("Error: No instruction to execute");
  *  After successful execution, call:
  *   log_running((pcb, "Running");
  *   log_queue((readyq.first, "Ready");
  *   log_queue((waitingq.first, "Waiting");
  *   log_queue((terminatedq.first, "Termianted");
  *   log_msg("\n");
  **/
void execute_instr(pcb_t *pcb) {
    if (!pcb || !pcb->next_instruction) {
        log_msg("Error: No instruction to execute");
        return;
    }

    if (pcb->next_instruction->type == REQ_OP) {
    request_resource(pcb);
    if (pcb->state == RUNNING)
    pcb->next_instruction = pcb->next_instruction->next;
    } else if (pcb->next_instruction->type == REL_OP) {
        release_resource(pcb);
        pcb->next_instruction = pcb->next_instruction->next;
    } else if (pcb->next_instruction->type == SEND_OP) {
        log_send(pcb->process->name, pcb->next_instruction->msg, pcb->next_instruction->resource_name);
        pcb->next_instruction = pcb->next_instruction->next;
    } else if (pcb->next_instruction->type == RECV_OP) {
        log_recv(pcb->process->name, pcb->next_instruction->msg, pcb->next_instruction->resource_name);
        pcb->next_instruction = pcb->next_instruction->next;
    } else {
        log_msg("Error: No instruction to execute");
    }

    if (pcb->state == RUNNING)
        log_running(pcb, "Running");
    log_running(pcb, "Running");
    log_queue(readyq.first, "Ready");
    log_queue(waitingq.first, "Waiting");
    log_queue(terminatedq.first, "Terminated");
    log_msg("\n");
}

/**
 * @brief Handle the request resource instruction
 *
 * If the resource could not be acquired move the process to the waiting queue
 * If the resource was successfully acquired call:
 *  log_request_acquired(cur_pcb->process->name, instr->resource_name);
 *  log_avail_resources(system_resources);
 *  log_msg("\n");
 */
void request_resource(pcb_t *cur_pcb) {
    if (!cur_pcb || !cur_pcb->next_instruction)
        return;
    char *resName = cur_pcb->next_instruction->resource_name;
    if (!resName)
        return;
    if (acquire_resource(cur_pcb, resName)) {
        log_request_acquired(cur_pcb->process->name, resName);
        log_avail_resources(system_resources);
        log_msg("\n");
    } else {
    cur_pcb->state = WAITING;
    enqueue_pcb(cur_pcb, &waitingq, WAITING);
#pragma omp critical(deadlock_check)
        {
            pcb_t *victim = detect_deadlock();
            if (victim) {
                pcb_t *prev = NULL, *cur = waitingq.first;
                while (cur) {
                    if (cur == victim) {
                        if (prev)
                            prev->next = cur->next;
                        else
                            waitingq.first = cur->next;
                        if (cur == waitingq.last)
                            waitingq.last = prev;
                        break;
                    }
                    prev = cur;
                    cur = cur->next;
                }
                victim->state = TERMINATED;
                enqueue_pcb(victim, &terminatedq, TERMINATED);
                procTerminated++;
            }
        }
    }
}

/**
 * @brief Acquire a resource for a process if it is available
 * NB: Do not remove the resource from the system_resources list
 *     Update the allocated field
 */
bool_t acquire_resource(pcb_t *cur_pcb, char *resource_name) {
if (!cur_pcb || !resource_name)
        return FALSE;
    resource_t *curRes = system_resources;
    while (curRes) {
        if (strcasecmp(curRes->name, resource_name) == 0) {
            if (curRes->allocated == NULL) {
                curRes->allocated = cur_pcb;
                return TRUE;
            } else
                return FALSE;
        }
        curRes = curRes->next;
    }
    return FALSE;
}

/**
 * @brief Execute the release instruction for the process
 *  Update the allocated field
 *  Find a process that is waiting for a resource with the same name and move it to the ready queue
 *
 * If the release was successful, call:
 *  log_release_released(pcb->process->name, resource_name);
 *  log_avail_resources(system_resources);
 *  log_msg("\n");
 * If the release was not successful, call:
 *  log_release_error(pcb->process->name, resource_name);
 *
 */
void release_resource(pcb_t *pcb) {
if (!pcb || !pcb->next_instruction)
        return;
    char *resName = pcb->next_instruction->resource_name;
    if (!resName)
        return;
    resource_t *curRes = system_resources;
    while (curRes) {
        if (strcmp(curRes->name, resName) == 0 && curRes->allocated == pcb) {
            curRes->allocated = NULL;
            log_release_released(pcb->process->name, resName);
            log_avail_resources(system_resources);
            log_msg("\n");
            pcb_t *prev = NULL;
            for (pcb_t *wp = waitingq.first; wp; wp = wp->next) {
                if (wp->next_instruction && wp->next_instruction->type == REQ_OP &&
                    strcmp(wp->next_instruction->resource_name, resName) == 0) {
                    if (prev)
                        prev->next = wp->next;
                    else
                        waitingq.first = wp->next;
                    if (wp == waitingq.last)
                        waitingq.last = prev;
                    wp->next = NULL;
                    wp->state = READY;
                    enqueue_pcb(wp, &readyq, READY);
                    return;
                }
                prev = wp;
            }
            return;
        }
        curRes = curRes->next;
    }
    log_release_error(pcb->process->name, resName);
}

/**
  * @brief Enqueue process <code>pcb</code> to <code>queue</code>
  * Log the enqueue operation appropiately, depending on <code>status</code>
    log_request_ready(pcb->process->name);
    log_request_waiting(pcb->process->name, pcb->next_instruction->resource_name);
    log_terminated(pcb->process->name);
  */
 void enqueue_pcb(pcb_t *pcb, pcb_queue_t *queue, int status) {
    pcb->state = status;
    switch (status) {
        case READY:
            log_request_ready(pcb->process->name);
            break;
        case WAITING:
            log_request_waiting(pcb->process->name, pcb->next_instruction ? pcb->next_instruction->resource_name : "UNKNOWN");
            break;
        case TERMINATED:
            log_terminated(pcb->process->name);
            break;
        default:
            return;
    }
    pcb->next = NULL;
    if (!queue->first) {
        queue->first = pcb;
        queue->last = pcb;
    } else {
        queue->last->next = pcb;
        queue->last = pcb;
    }
}

 /** Dequeue process pcb from queue <code>queue</code>. */
pcb_t *dequeue_pcb(pcb_queue_t *queue) {
    pcb_t *front = queue->first;
    if (!front)
        return NULL;
    queue->first = front->next;
    if (!queue->first)
        queue->last = NULL;
    front->next = NULL;
    return front;
}

/** @brief Deallocate the queues */
void free_manager(void) {
    log_queue(readyq.first, "Ready");
    log_queue(waitingq.first, "Waiting");
    log_queue(terminatedq.first, "Terminated");

    #ifdef DEBUG_MNGR
    printf("\nFreeing the queues...\n");
    #endif
    dealloc_pcbs(readyq.first);
    dealloc_pcbs(waitingq.first);
    dealloc_pcbs(terminatedq.first);
}

/** @brief Retrieve the number of threads to create from the list of arguments */
int get_num_threads(int num_args, char **argv) {
    if (num_args > 1) return atoi(argv[1]);
        else return 1;
    }

    /** @brief Retrieve the name of a process file or the codename "generate" from the list of arguments */
    char *get_data(int num_args, char **argv) {
    char *data_origin = "generate";
    if (num_args > 2) return argv[2];
    else return data_origin;
}

/** @brief Retrieve the scheduler algorithm type from the list of arguments */
int get_algo(int num_args, char **argv) {
    if (num_args > 3) return atoi(argv[3]);
    else return 1;
}

/** @brief Retrieve the time quantum from the list of arguments */
int get_time_quantum(int num_args, char **argv) {
    if (num_args > 4) return atoi(argv[4]);
    else return 1;
}

/** @brief Print the arguments of the program */
void print_args(int num_thr, char *data, int sched, int tq) {
    printf("Arguments: num_threads = %d, data = %s, scheduler = %s,  time quantum = %d\n", num_thr, data, (sched==0)?"priority":"RR", tq);
}
