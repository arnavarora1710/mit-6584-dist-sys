package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskState int

const (
	IDLE TaskState = iota
	IN_PROGRESS
	DONE
)

type Task struct {
	TaskNum   int
	IsMap     bool
	TaskState TaskState
	File      string
}

type Coordinator struct {
	// list of tasks
	Tasks []Task
	// max number of reduce tasks
	NReduce int
	// boolean array to track if reduce tasks are done
	ReduceDone []bool
}

func GiveMapTask(c *Coordinator, reply *TaskReply) bool {
	reply.NReduce = c.NReduce

	found_idle := false
	for _, task := range c.Tasks {
		if task.IsMap && task.TaskState == IDLE {
			task.TaskState = IN_PROGRESS
			reply.Task = task
			found_idle = true
			break
		}
	}
	return found_idle
}

func GiveReduceTask(c *Coordinator, reply *TaskReply) bool {
	found_idle := false
	for _, task := range c.Tasks {
		if !task.IsMap && task.TaskState == IDLE {
			task.TaskState = IN_PROGRESS
			reply.Task = task
			found_idle = true
			break
		}
	}
	return found_idle
}

func (c *Coordinator) GiveTask(args *TaskArgs, reply *TaskReply) error {
	if !GiveMapTask(c, reply) {
		GiveReduceTask(c, reply)
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	if args.IsMap {
		c.Tasks[args.TaskNum].TaskState = DONE
		// all files of the shape mr-<task_num>-<reduce_task_num>
		// reduce_task_num goes from 0 to NReduce - 1
		for reduce_task_num := 0; reduce_task_num < c.NReduce; reduce_task_num++ {
			intermediate_file_name := fmt.Sprintf("mr-%d-%d", args.TaskNum, reduce_task_num)
			if c.ReduceDone[reduce_task_num] {
				continue
			}
			// if the reduce task is not done, add it to the list of tasks
			fmt.Printf("adding reduce task %d\n", reduce_task_num)
			c.ReduceDone[reduce_task_num] = true
			c.Tasks = append(c.Tasks, Task{
				TaskNum:   len(c.Tasks),
				IsMap:     false,
				TaskState: IDLE,
				File:      intermediate_file_name,
			})
		}
	} else {
		c.Tasks[args.TaskNum].TaskState = DONE
	}
	return nil
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
	for _, task := range c.Tasks {
		if task.IsMap && task.TaskState != DONE {
			return false
		}
	}

	fmt.Printf("all map tasks are done\n")

	for _, task := range c.Tasks {
		if !task.IsMap && task.TaskState != DONE {
			return false
		}
	}

	fmt.Printf("all reduce tasks are done\n")

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NReduce = nReduce
	c.ReduceDone = make([]bool, nReduce)

	for i, file := range files {
		c.Tasks = append(c.Tasks, Task{
			TaskNum:   i,
			IsMap:     true,
			TaskState: IDLE,
			File:      file,
		})
	}

	c.server()
	return &c
}
