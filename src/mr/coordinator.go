package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Done
)

type MapTask struct {
	filename  string
	status    int // 0: not started, 1: in progress, 2: completed
	startedAt int64
}

type ReduceTask struct {
	id        int // maps directly to the index of nReduce
	status    int // 0: not started, 1: in progress, 2: completed
	startedAt int64
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []MapTask // logically corresponds to how many files are passed in
	reduceTasks []ReduceTask

	// cache of number of files
	nMap int
	// cache of number of reduce tasks
	nReduce int
	phase   Phase
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {

	// first check if there are any map tasks that are not completed
	// TODO: looping on mapTasks is  probably not the best way to do this,
	// but it's simple and works for now
	mapTasksCompleted := 0
	reduceTasksCompleted := 0
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reset any tasks that have been in progress for too long (10 seconds)
	// back to not started
	for i, task := range c.mapTasks {
		if task.status == 1 && time.Now().Unix()-task.startedAt > 10 {
			c.mapTasks[i].status = 0
		}
	}

	for i, task := range c.reduceTasks {
		if task.status == 1 && time.Now().Unix()-task.startedAt > 10 {
			c.reduceTasks[i].status = 0
		}
	}

	switch c.phase {
	case MapPhase:
		for i, task := range c.mapTasks {
			if task.status == 0 {
				// assign this map task to the worker
				c.mapTasks[i].status = 1
				c.mapTasks[i].startedAt = time.Now().Unix()
				reply.Type = TaskMap
				reply.ID = i
				reply.Filename = task.filename
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}

			if task.status == 2 {
				mapTasksCompleted++
			}
		}
		if mapTasksCompleted == c.nMap {
			// all map tasks are completed, move to reduce phase
			c.phase = ReducePhase
		}
	case ReducePhase:
		for i, task := range c.reduceTasks {
			if task.status == 0 {
				c.reduceTasks[i].status = 1
				c.reduceTasks[i].startedAt = time.Now().Unix()
				reply.Type = TaskReduce
				reply.ID = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
			if task.status == 2 {
				reduceTasksCompleted++
			}
		}
		if reduceTasksCompleted == c.nReduce {
			// all reduce tasks are completed, move to done phase
			c.phase = Done
		}
	case Done:
		reply.Type = TaskExit
		return nil
	default:
		log.Fatalf("Coordinator: unknown phase %v", c.phase)
	}

	// For now just return a wait task if there are no map tasks to assign
	reply.Type = TaskWait
	return nil
}

func (c *Coordinator) CompleteTask(args *TaskCompletion, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case TaskMap:
		// Check status of the map task and update it to completed
		switch args.Status {
		case TaskComplete:
			c.mapTasks[args.ID].status = 2
		case TaskFailed, TaskIncomplete:
			c.mapTasks[args.ID].status = 0 // reset to not started
		}
	case TaskReduce:
		switch args.Status {
		case TaskComplete:
			c.reduceTasks[args.ID].status = 2
		case TaskFailed, TaskIncomplete:
			c.reduceTasks[args.ID].status = 0 // reset to not started
		}
	default:
		log.Fatalf("Coordinator: cannot complete task type %v", args.Type)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)
	for i, filename := range files {
		c.mapTasks[i] = MapTask{filename: filename, status: 0}
	}
	for i, _ := range c.reduceTasks {
		c.reduceTasks[i] = ReduceTask{id: i, status: 0}
	}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.phase = MapPhase

	c.server(sockname)
	return &c
}
