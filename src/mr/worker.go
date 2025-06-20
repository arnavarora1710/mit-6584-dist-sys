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
	"strconv"
	"strings"
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

func DoMapTask(task Task, mapf func(string, string) []KeyValue, nReduce int) {
	file_name := task.File

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
		intermediate_files[i], err = os.Create(fmt.Sprintf("mr-%d-%d", task.TaskNum, i))
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
	TaskDone(task.TaskNum, true)
}

func DoReduceTask(task Task, reducef func(string, []string) string) {
	file_name := task.File

	// read file
	file, err := os.Open(file_name)
	if err != nil {
		log.Fatalf("cannot open %v", file_name)
	}
	dec := json.NewDecoder(file)
	intermediate := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	file.Close()

	// sort key value pairs
	sort.Sort(ByKey(intermediate))

	// make reduce file if it doesn't exist
	reduce_num, err := strconv.Atoi(task.File[strings.LastIndex(task.File, "-")+1:])
	if err != nil {
		log.Fatalf("cannot convert reduce file name to int %v", task.File)
	}
	reduce_file, err := os.OpenFile(fmt.Sprintf("mr-out-%d", reduce_num), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("cannot create reduce file %v", reduce_num)
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
	TaskDone(task.TaskNum, false)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		task_reply := GetTask()
		task := task_reply.Task
		nReduce := task_reply.NReduce

		if task.IsMap {
			DoMapTask(task, mapf, nReduce)
		} else {
			DoReduceTask(task, reducef)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TaskDone(task_num int, is_map bool) {
	args := TaskDoneArgs{
		TaskNum: task_num,
		IsMap:   is_map,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		fmt.Printf("task %d is done\n", task_num)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func GetTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.GiveTask", &args, &reply)
	if ok {
		fmt.Printf("reply.task %v\n", reply.Task)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
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
