package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
)

func DoMapTask(map_task MapTask, mapf func(string, string) []KeyValue, nReduce int) {
	file_name := map_task.File

	// read file
	file, err := os.Open(file_name)
	if err != nil {
		log.Fatalf("cannot open %v", file_name)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file_name)
	}
	file.Close()

	// run map function on file and get list of key value pairs
	kva := mapf(file_name, string(content))

	// all intermediate files map to a reduce task
	// map each key value pair to a reduce task file
	map_file_to_kvs := make(map[int][]KeyValue)
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		map_file_to_kvs[index] = append(map_file_to_kvs[index], kv)
	}

	// create intermediate files
	intermediate_files := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		// intermediate file name: mr-<map_task_num>-<reduce_task_num>
		intermediate_files[i], err = os.Create(fmt.Sprintf("mr-%d-%d", map_task.TaskNum, i))
		if err != nil {
			log.Fatalf("cannot create intermediate file %v", i)
		}

		// push key value pairs corresponding to the current intermediate file
		enc := json.NewEncoder(intermediate_files[i])
		for _, kv := range map_file_to_kvs[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode intermediate file %v", i)
			}
		}
		intermediate_files[i].Close()
	}
	// rpc the coordinator that the map task is done
	MapTaskDone(map_task.TaskNum)
}

func (c *Coordinator) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	reply.NReduce = c.NReduce

	for i, task := range c.MapTasks {
		// make this section atomic
		// if a worker has already claimed a task, don't give it to another worker
		mu.Lock()
		if task.TaskState == IDLE_READY {
			c.MapTasks[i].TaskState = IN_PROGRESS
			reply.MapTask = c.MapTasks[i]
			mu.Unlock()
			return nil
		}
		mu.Unlock()
	}
	reply.MapTask = MapTask{
		TaskNum:   -1,
		TaskState: NO_MORE_TASKS,
		File:      "",
	}
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	mu.Lock()
	c.MapTasks[args.TaskNum].TaskState = DONE
	// init ready reduce tasks
	for reduce_task_num := 0; reduce_task_num < c.NReduce; reduce_task_num++ {
		intermediate_file_name := fmt.Sprintf("mr-%d-%d", args.TaskNum, reduce_task_num)
		// if the intermediate file is not already in the reduce task, add it
		if !slices.Contains(c.ReduceTasks[reduce_task_num].Files, intermediate_file_name) {
			c.ReduceTasks[reduce_task_num].Files = append(c.ReduceTasks[reduce_task_num].Files, intermediate_file_name)

			if len(c.ReduceTasks[reduce_task_num].Files) == c.NMap {
				c.ReduceTasks[reduce_task_num].TaskState = IDLE_READY
			}
		}
	}
	mu.Unlock()
	return nil
}

func GetMapTask() MapTaskReply {
	args := MapTaskArgs{}
	reply := MapTaskReply{}
	call("Coordinator.GiveMapTask", &args, &reply)
	return reply
}

func MapTaskDone(task_num int) {
	args := MapTaskDoneArgs{
		TaskNum: task_num,
	}
	reply := MapTaskDoneReply{}
	call("Coordinator.MapTaskDone", &args, &reply)
}
