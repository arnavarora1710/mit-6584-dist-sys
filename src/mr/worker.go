package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func initWorker() int {
	worker_id := os.Getpid()
	worker_init_args := WorkerInitArgs{
		WorkerId: worker_id,
	}
	worker_init_reply := WorkerInitReply{}
	call("Coordinator.WorkerInit", &worker_init_args, &worker_init_reply)
	return worker_id
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker_id := initWorker()
	for {
		map_task_reply := GetMapTask(worker_id)
		map_task := map_task_reply.MapTask

		if map_task.TaskState == NO_MORE_TASKS {
			break
		} else if map_task.TaskState == NO_TASK_READY {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		nReduce := map_task_reply.NReduce

		fmt.Printf("worker %v working on map task: %v\n", os.Getpid(), map_task)

		DoMapTask(worker_id, map_task, mapf, nReduce)
		time.Sleep(10 * time.Millisecond)
	}

	for {
		reduce_task_ask := GetReduceTask(worker_id)
		reduce_task := reduce_task_ask.ReduceTask

		if reduce_task.TaskState == NO_MORE_TASKS {
			os.Exit(0)
		} else if reduce_task.TaskState == NO_TASK_READY {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		fmt.Printf("worker %v working on reduce task: %v\n", os.Getpid(), reduce_task)

		DoReduceTask(worker_id, reduce_task, reducef)
		time.Sleep(10 * time.Millisecond)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
