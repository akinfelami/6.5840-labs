package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskRequest struct {
}

type TaskType int
type TaskCompletionStatus int

const (
	TaskInvalid TaskType = iota
	TaskMap
	TaskReduce
	TaskWait
	TaskExit
)

const (
	TaskIncomplete TaskCompletionStatus = iota
	TaskComplete
	TaskFailed
)

type TaskReply struct {
	Type     TaskType
	ID       int
	Filename string
	NReduce  int
	NMap     int
}

type TaskCompletion struct {
	ID     int
	Type   TaskType
	Status TaskCompletionStatus
}
