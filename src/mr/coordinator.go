package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex

type TaskState int

const (
	IDLE_NOT_READY TaskState = iota
	IDLE_READY
	IN_PROGRESS
	DONE
	NO_MORE_TASKS
	NO_TASK_READY
)

type MapTask struct {
	TaskNum   int
	TaskState TaskState
	File      string
	WorkerId  int
}

type ReduceTask struct {
	TaskNum   int
	TaskState TaskState
	Files     []string
	WorkerId  int
}

type WorkerState int

const (
	WORKER_IDLE WorkerState = iota
	WORKER_IN_PROGRESS
	WORKER_CRASHED
)

type WorkerInfo struct {
	WorkerId     int
	State        WorkerState
	TimeAssigned time.Time
	MapTask      MapTask
	ReduceTask   ReduceTask
}

type Coordinator struct {
	// list of map tasks
	MapTasks []MapTask
	// list of reduce tasks
	ReduceTasks []ReduceTask
	// max number of reduce tasks
	NReduce int
	// number of map tasks
	NMap int
	// list of workers
	Workers map[int]WorkerInfo
}

func (c *Coordinator) WorkerInit(args *WorkerInitArgs, reply *WorkerInitReply) error {
	mu.Lock()
	fmt.Printf("WorkerInit: %v\n", args.WorkerId)
	c.Workers[args.WorkerId] = WorkerInfo{
		WorkerId:     args.WorkerId,
		State:        WORKER_IDLE,
		TimeAssigned: time.Now(),
		MapTask:      MapTask{},
		ReduceTask:   ReduceTask{},
	}
	mu.Unlock()
	return nil
}

// start a thread that checks on workers
func (c *Coordinator) checkOnWorkers() {
	go func() {
		for {
			mu.Lock()
			for worker_id, worker_info := range c.Workers {
				if worker_info.State == WORKER_IN_PROGRESS && time.Since(worker_info.TimeAssigned) > 10*time.Second {
					if worker_info.MapTask.TaskState == IN_PROGRESS {
						c.MapTasks[worker_info.MapTask.TaskNum].TaskState = IDLE_READY
					} else if worker_info.ReduceTask.TaskState == IN_PROGRESS {
						c.ReduceTasks[worker_info.ReduceTask.TaskNum].TaskState = IDLE_READY
					}
					c.Workers[worker_id] = WorkerInfo{
						WorkerId:     worker_id,
						State:        WORKER_CRASHED,
						TimeAssigned: time.Now(),
						MapTask:      MapTask{},
						ReduceTask:   ReduceTask{},
					}
				}
			}
			mu.Unlock()
		}
	}()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// check if all reduce tasks are done
	for _, task := range c.MapTasks {
		if task.TaskState != DONE {
			return false
		}
	}

	for _, task := range c.ReduceTasks {
		if task.TaskState != DONE {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NReduce = nReduce
	c.NMap = len(files)
	c.Workers = make(map[int]WorkerInfo)

	for i, file := range files {
		c.MapTasks = append(c.MapTasks, MapTask{
			TaskNum:   i,
			TaskState: IDLE_READY,
			File:      file,
			WorkerId:  -1,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, ReduceTask{
			TaskNum:   i,
			TaskState: IDLE_NOT_READY,
			Files:     []string{},
			WorkerId:  -1,
		})
	}

	c.server()
	c.checkOnWorkers()
	return &c
}
