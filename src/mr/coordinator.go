package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// MapRecuce全局状态
type GlobalStatus uint

const (
	gMap    GlobalStatus = 1
	gReduce GlobalStatus = 2
	gDone   GlobalStatus = 3
)

// 任务的状态
type TaskStatus uint

const (
	Idle TaskStatus = 1
	Run  TaskStatus = 2
	Done TaskStatus = 3
)

type Coordinator struct {
	// Your definitions here.
	Status     GlobalStatus // 状态
	InputFiles []string
	MMap       int
	MapTask    []TaskStatus // 所有Map任务的状态
	NReduce    int
	ReduceTask []TaskStatus // 所有Reduce任务的状态
	InnerMutex sync.Mutex   // 保护整个结构体的锁
}

// Your code here -- RPC handlers for the worker to call.

// 取出空闲的任务
func GetIdleTask(t []TaskStatus) int {
	for k, v := range t {
		if v == Idle {
			t[k] = Run
			return k
		}
	}
	return -1
}

// 获得Reduce任务所需要读取的所有中间文件的名字
func GetReduceFiles(M int, N int) []string {
	ret := []string{}
	file := "mr-out-"
	for i := 0; i < M; i++ {
		ret = append(ret, file+strconv.Itoa(i)+"-"+strconv.Itoa(N))
	}
	return ret
}

// 分配任务
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *TaskReply) error {
	c.InnerMutex.Lock()
	defer c.InnerMutex.Unlock()

	reply.NReduce = c.NReduce
	index := 0
	if c.Status == gMap {
		if index = GetIdleTask(c.MapTask); index != -1 {
			reply.Index = index
			reply.Type = MapType
			reply.Files = append(reply.Files, c.InputFiles[index])
		} else {
			reply.Type = SleepType
		}
	} else if c.Status == gReduce {
		if index = GetIdleTask(c.ReduceTask); index != -1 {
			reply.Index = index
			reply.Type = ReduceType
			reply.Files = GetReduceFiles(c.MMap, index)
		} else {
			reply.Type = SleepType
		}
	} else {
		reply.Type = DoneType
	}

	// 检查10秒后该任务有没有完成，若没有则认为worker宕机，并将任务状态重置。
	if reply.Type == MapType || reply.Type == ReduceType {
		go func(index int, t TaskType) {
			time.Sleep(10 * time.Second)

			c.InnerMutex.Lock()
			defer c.InnerMutex.Unlock()

			if t == MapType && c.MapTask[index] != Done {
				c.MapTask[index] = Idle
			}
			if t == ReduceType && c.ReduceTask[index] != Done {
				c.ReduceTask[index] = Idle
			}
		}(index, reply.Type)
	}

	return nil
}

// 检查是否所有的任务都已完成
func checkAllDone(t []TaskStatus) bool {
	for _, v := range t {
		if v != Done {
			return false
		}
	}
	return true
}

// 接收任务完成的通知
func (c *Coordinator) ReceiveDoneMessage(args *TaskDone, reply *ExampleReply) error {
	c.InnerMutex.Lock()
	defer c.InnerMutex.Unlock()

	if args.Type == MapType && c.MapTask[args.Index] != Done {
		c.MapTask[args.Index] = Done
		if checkAllDone(c.MapTask) {
			c.Status = gReduce
		}
	}
	if args.Type == ReduceType && c.ReduceTask[args.Index] != Done {
		c.ReduceTask[args.Index] = Done
		if checkAllDone(c.ReduceTask) {
			c.Status = gDone
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.InnerMutex.Lock()
	defer c.InnerMutex.Unlock()
	ret = (c.Status == gDone)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化coordinator数据结构
	c.Status = gMap
	c.InputFiles = files
	c.MMap = len(files)
	c.NReduce = nReduce
	c.MapTask = make([]TaskStatus, c.MMap)
	c.ReduceTask = make([]TaskStatus, c.NReduce)
	for i := 0; i < len(files); i++ {
		c.MapTask[i] = Idle
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTask[i] = Idle
	}

	c.server()
	return &c
}
