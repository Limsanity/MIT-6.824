package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func callReportTask(taskIdx int, status TaskStatus, phase TaskPhase) {
	reportArgs := ReportTaskArgs{ TaskIdx: taskIdx, Status: status, Phase: phase }
	reportReply := ReportTaskReply{}
	call("Coordinator.ReportTask", &reportArgs, &reportReply)
}

func doMapTask(mapf func(string, string) []KeyValue, reply RequestTaskReply) {
	taskIdx := reply.Task.Idx
	phase := reply.Task.Phase
	filename := reply.Task.Filename

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		callReportTask(taskIdx, Error, phase)
		return
	}

	kva := mapf(filename, string(content))
	reduces := make([][]KeyValue, reply.NReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, reduce := range reduces {
		filename := fmt.Sprintf("mr-%d-%d", taskIdx, idx)
		tmpFilename := fmt.Sprintf("%s.tmp", filename)

		fd, err := os.Create(tmpFilename)
		if err != nil {
			callReportTask(taskIdx, Error, phase)
			return
		}

		enc := json.NewEncoder(fd)
		
		for _, kv := range reduce {
			if err := enc.Encode(&kv); err != nil {
				callReportTask(taskIdx, Error, phase)
				return
			}
		}

		if err := fd.Close(); err != nil {
			callReportTask(taskIdx, Error, phase)
			return
		}

		os.Rename(tmpFilename, filename)
	}

	callReportTask(taskIdx, Finished, phase)
}

func doReduceTask(reducef func(string, []string) string, reply RequestTaskReply) {
	taskIdx := reply.Task.Idx
	phase := reply.Phase

	maps := make(map[string][]string)

	for idx := 0; idx < reply.NMap; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, taskIdx)

		fd, err := os.Open(filename)
		if err != nil {
			callReportTask(taskIdx, Error, phase)
			return
		}

		dec := json.NewDecoder(fd)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, reducef(k, v)))
	}

	if err := ioutil.WriteFile(fmt.Sprintf("mr-out-%d", taskIdx), []byte(strings.Join(res, "")), 0600); err != nil {
		callReportTask(taskIdx, Error, phase)
		return
	}

	callReportTask(taskIdx, Finished, phase)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)

		if ok {
			if reply.Task == nil {
				continue
			}

			if reply.Phase == MapPhase {
				doMapTask(mapf, reply)
			} else if reply.Phase == ReducePhase {
				doReduceTask(reducef, reply)
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	log.Fatal(err)
	return false
}
