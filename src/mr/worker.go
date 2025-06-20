package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		map_task_reply := GetMapTask()
		fmt.Printf("map task reply: %v\n", map_task_reply)
		map_task := map_task_reply.MapTask

		if map_task.TaskState == NO_MORE_TASKS {
			break
		}

		nReduce := map_task_reply.NReduce

		DoMapTask(map_task, mapf, nReduce)
		time.Sleep(10 * time.Millisecond)
	}

	for {
		reduce_task_ask := GetReduceTask()
		reduce_task := reduce_task_ask.ReduceTask

		if reduce_task.TaskState == NO_MORE_TASKS {
			os.Exit(0)
		} else if reduce_task.TaskState == NO_TASK_READY {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		DoReduceTask(reduce_task, reducef)
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
