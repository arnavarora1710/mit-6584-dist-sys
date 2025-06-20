package mr

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
