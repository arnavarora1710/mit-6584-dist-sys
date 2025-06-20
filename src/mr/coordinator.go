package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
}

type ReduceTask struct {
	TaskNum   int
	TaskState TaskState
	Files     []string
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

	for i, file := range files {
		c.MapTasks = append(c.MapTasks, MapTask{
			TaskNum:   i,
			TaskState: IDLE_READY,
			File:      file,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, ReduceTask{
			TaskNum:   i,
			TaskState: IDLE_NOT_READY,
			Files:     []string{},
		})
	}

	c.server()
	return &c
}
