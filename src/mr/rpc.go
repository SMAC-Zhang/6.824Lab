package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// 任务类型
type TaskType uint

const (
	MapType    TaskType = 1
	ReduceType TaskType = 2
	SleepType  TaskType = 3
	DoneType   TaskType = 4
)

// coordinator分配任务
type TaskReply struct {
	NReduce int
	Index   int
	Type    TaskType
	Files   []string
}

// worker完成任务后告诉coordinator
type TaskDone struct {
	Index int
	Type  TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
