package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func DoReduceTask(reduce_task ReduceTask, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, file_name := range reduce_task.Files {
		// read file
		file, err := os.Open(file_name)
		if err != nil {
			log.Fatalf("cannot open %v", file_name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort key value pairs
	sort.Sort(ByKey(intermediate))

	// make reduce file if it doesn't exist
	reduce_file, err := os.OpenFile(fmt.Sprintf("mr-out-%d", reduce_task.TaskNum), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("cannot create reduce file %v", reduce_task.TaskNum)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(reduce_file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	reduce_file.Close()

	// rpc the coordinator that the reduce task is done
	ReduceTaskDone(reduce_task.TaskNum)
}

func (c *Coordinator) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	// make this section atomic
	// if a worker has already claimed a task, don't give it to another worker
	mu.Lock()
	found_ready := false
	found_idle_not_ready := false
	for i, task := range c.ReduceTasks {
		if task.TaskState == IDLE_READY {
			c.ReduceTasks[i].TaskState = IN_PROGRESS
			reply.ReduceTask = c.ReduceTasks[i]
			found_ready = true
			break
		} else if task.TaskState == IDLE_NOT_READY {
			found_idle_not_ready = true
		}
	}
	mu.Unlock()
	if found_ready {
		return nil
	}

	var task_state TaskState
	if found_idle_not_ready {
		task_state = NO_TASK_READY
	} else {
		task_state = NO_MORE_TASKS
	}

	reply.ReduceTask = ReduceTask{
		TaskNum:   -1,
		TaskState: task_state,
		Files:     []string{},
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	mu.Lock()
	c.ReduceTasks[args.TaskNum].TaskState = DONE
	mu.Unlock()
	return nil
}

func GetReduceTask() ReduceTaskReply {
	args := ReduceTaskArgs{}
	reply := ReduceTaskReply{}
	call("Coordinator.GiveReduceTask", &args, &reply)
	return reply
}

func ReduceTaskDone(task_num int) {
	args := ReduceTaskDoneArgs{
		TaskNum: task_num,
	}
	reply := ReduceTaskDoneReply{}
	call("Coordinator.ReduceTaskDone", &args, &reply)
}
