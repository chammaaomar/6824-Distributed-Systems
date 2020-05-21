package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

// Worker asks the Master for a task via the RequestTask
// RPC. It then begins working on the file provided by
// the Master. If the Master doesn't respond within
// 10 seconds, it exits. If the Master responds
// with nothing, it sleeps.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	taskInfo := TaskResponse{}
	taskArgs := TaskArgs{}
	for {
		callSuccess := call("Master.RequestTask", taskArgs, &taskInfo)
		if callSuccess {
			if taskInfo.TaskType == Map {
				handleMap(mapf, taskInfo.Filename, taskInfo.NReduce, taskInfo.TaskID)
				// task successfully completed, let the master know
				done := DoneArgs{taskInfo.TaskType, taskInfo.Filename}
				doneRes := DoneResponse{}
				call("Master.NotifyDone", done, &doneRes)
			} else {
				// Reduce
				os.Exit(1)
			}
		}
	}

}

func handleMap(mapf func(string, string) []KeyValue, file string, nreduce int, taskID int) {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("worker reading: %v", err)
		return
	}
	// intermediate filename "mr-X-R"
	var iFilename string
	intermediateFiles := make([]*os.File, nreduce)
	enc := make([]*json.Encoder, nreduce)
	kvsEmitted := mapf(file, string(contents))

	for i := range intermediateFiles {
		iFilename = fmt.Sprintf("mr-%d-%d", taskID, i)
		intermediateFiles[i], err = os.Create(iFilename)
		defer intermediateFiles[i].Close()
		if err != nil {
			fmt.Printf("worker creating intermediate: %v", err)
			return
		}
		enc[i] = json.NewEncoder(intermediateFiles[i])
	}

	for _, kv := range kvsEmitted {
		reducerID := ihash(kv.Key) % nreduce
		err := enc[reducerID].Encode(&kv)
		if err != nil {
			fmt.Printf("worker writing intermediate: %v", err)
			return
		}
	}

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
