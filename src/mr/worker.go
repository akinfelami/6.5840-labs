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
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func listReducerFiles(id int) ([]string, error) {
	suffix := fmt.Sprintf("-%d", id)
	entries, err := os.ReadDir(".")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "mr-") || !strings.HasSuffix(name, suffix) {
			continue
		}
		rest := strings.TrimSuffix(strings.TrimPrefix(name, "mr-"), suffix)
		if _, err := strconv.Atoi(rest); err != nil {
			continue
		}
		files = append(files, name)
	}
	return files, nil
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		reply := AskForTask()
		switch reply.Type {
		case TaskWait:
			log.Printf("Worker: received wait task, not doing anything...")
			continue
		case TaskExit:
			log.Printf("Worker: received exit task, exiting")
			return
		case TaskMap:
			taskCompletion := TaskCompletion{ID: reply.ID, Type: TaskMap, Status: TaskIncomplete}
			err := AttemptMapTask(reply, &taskCompletion, mapf)
			if err != nil {
				log.Printf("Worker: map task %d failed with error: %v", reply.ID, err)
				taskCompletion.Status = TaskFailed
			}
			ok := call("Coordinator.CompleteTask", &taskCompletion, &struct{}{})
			if !ok {
				log.Printf("Worker: failed to report completion of map task %d", reply.ID)
			}
		case TaskReduce:
			log.Printf("Worker: received reduce task %d", reply.ID)
			taskCompletion := TaskCompletion{ID: reply.ID, Type: TaskReduce, Status: TaskIncomplete}
			err := AttemptReduceTask(reply, &taskCompletion, reducef)
			if err != nil {
				log.Printf("Worker: reduce task %d failed with error: %v", reply.ID, err)
				taskCompletion.Status = TaskFailed
			}
			ok := call("Coordinator.CompleteTask", &taskCompletion, &struct{}{})
			if !ok {
				log.Printf("Worker: failed to report completion of reduce task %d", reply.ID)
			}
		default:
			log.Printf("Worker: received unknown task type %v", reply.Type)
		}
	}
}

func AttemptReduceTask(reply *TaskReply, result *TaskCompletion, reducef func(string, []string) string) error {
	intermediate := []KeyValue{}

	files, err := listReducerFiles(reply.ID)
	if err != nil {
		return err
	}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.ID)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	*result = TaskCompletion{ID: reply.ID, Type: TaskReduce, Status: TaskComplete}
	return nil

}

func AttemptMapTask(reply *TaskReply, result *TaskCompletion, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.Filename)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()
	kva := mapf(reply.Filename, string(content))
	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.ID, i)
		ofile, _ := os.CreateTemp(".", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			if ihash(kv.Key)%reply.NReduce == i {
				enc.Encode(&kv)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
	*result = TaskCompletion{ID: reply.ID, Type: TaskMap, Status: TaskComplete}
	return nil
}

func AskForTask() *TaskReply {
	args := TaskRequest{}

	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		log.Printf("Worker: received task %v", reply)
	} else {
		log.Printf("call failed!\n")
	}
	return &reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
