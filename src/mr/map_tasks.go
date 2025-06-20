package mr

import (
	"fmt"
	"slices"
)

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
