package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	nil_args := ExampleArgs{}
	reply := TaskReply{}
	// 如果没申请到任务，就睡一会在申请
	for {
		reply = TaskReply{}
		call("Coordinator.AssignTask", &nil_args, &reply)
		if reply.Type != SleepType {
			break
		}
		time.Sleep(time.Second)

	}

	// 如果没有任务了，则退出
	if reply.Type == DoneType {
		return
	}

	// 处理任务
	if reply.Type == MapType {
		MapProc(mapf, &reply)
	}
	if reply.Type == ReduceType {
		ReduceProc(reducef, reply.Index, reply.Files)
	}

	// 任务完成，告知coordinator
	args := TaskDone{Index: reply.Index, Type: reply.Type}
	nil_reply := ExampleReply{}
	call("Coordinator.ReceiveDoneMessage", &args, &nil_reply)

	Worker(mapf, reducef)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func MapProc(mapf func(string, string) []KeyValue, args *TaskReply) {
	// 读文件
	filename := args.Files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// map
	intermediate := mapf(filename, string(content))

	// 创建中间文件
	ofiles := []*os.File{}
	encs := []*json.Encoder{}
	for i := 0; i < args.NReduce; i++ {
		ofile, _ := ioutil.TempFile("./", "mr-out-")
		ofiles = append(ofiles, ofile)
		enc := json.NewEncoder(ofile)
		encs = append(encs, enc)
	}

	// 写入对应文件
	for _, kv := range intermediate {
		enc := encs[ihash(kv.Key)%args.NReduce]
		enc.Encode(&kv)
	}

	for i, ofile := range ofiles {
		oname := "mr-out-" + strconv.Itoa(args.Index) + "-" + strconv.Itoa(i)
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}
}

func ReduceProc(reducef func(string, []string) string, index int, files []string) {
	// 从中间文件读出键值对
	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// 排序
	sort.Sort(ByKey(kva))
	// call redece 并写入结果
	oname := "mr-out-" + strconv.Itoa(index)
	ofile, _ := ioutil.TempFile("./", oname)
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	// 原子重命名
	os.Rename(ofile.Name(), oname)
	// 删除中间文件
	for _, filename := range files {
		os.Remove(filename)
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
