package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
)

// KeyValue is used to decode key-value pairs
// emitted by Map
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

// Worker asks the Master for a task via the RequestTask
// RPC. It then begins working on the file provided by
// the Master. If the Master doesn't respond within
// 10 seconds, it exits. If the Master responds
// with nothing, it sleeps.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	taskInfo := TaskResponse{}
	taskArgs := TaskArgs{}
	for {
		callError := call("Master.RequestTask", taskArgs, &taskInfo)
		if callError == nil {
			if taskInfo.TaskType == Map {
				handleMap(mapf, taskInfo.Filename, taskInfo.NReduce, taskInfo.TaskID)
				// task successfully completed, let the master know
				done := DoneArgs{taskInfo.TaskType, taskInfo.Filename}
				doneRes := DoneResponse{}
				call("Master.NotifyDone", done, &doneRes)
			} else {
				// Reduce
				handleReduce(reducef, taskInfo.Filename)
				// task successfully completed, let the master know
				done := DoneArgs{taskInfo.TaskType, taskInfo.Filename}
				doneRes := DoneResponse{}
				call("Master.NotifyDone", done, &doneRes)
			}
		} else if callError == ErrDone {
			// no tasks available, wait for a sec before asking
			fmt.Printf("worker calling master: %v\n", callError)
			fmt.Println("worker: Master not responding. Exiting...")
			os.Exit(0)
		}
	}

}

func handleMap(mapf func(string, string) []KeyValue, file string, nreduce int, taskID int) {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("mapper reading: %v", err)
		return
	}

	var iFilename string
	intermediateFiles := make([]*os.File, nreduce)
	// json encoder for every intermediate file
	enc := make([]*json.Encoder, nreduce)
	kvsEmitted := mapf(file, string(contents))

	// intermediate filename "mr-X-R"
	for i := range intermediateFiles {
		iFilename = fmt.Sprintf("mr-worker-%d-%d", taskID, i)
		intermediateFiles[i], err = os.Create(iFilename)
		defer intermediateFiles[i].Close()
		if err != nil {
			fmt.Printf("mapper creating intermediate: %v", err)
			return
		}
		enc[i] = json.NewEncoder(intermediateFiles[i])
	}

	for _, kv := range kvsEmitted {
		reducerID := ihash(kv.Key) % nreduce
		err := enc[reducerID].Encode(&kv)
		if err != nil {
			fmt.Printf("mapper writing intermediate: %v", err)
			return
		}
	}

}

// handleReduce reads the intermediate files output by all the mappers
// with keys whose hash values map to the specific reducer. It starts
// writing its output to an intermediate file, then atomically renames
// once it's done.
func handleReduce(reducef func(string, []string) string, globPattern string) {
	// find all intermediate files output by mappers with keys
	// belonging to this specific reducer
	mapOutputs, err := filepath.Glob(globPattern)
	lastDash := strings.LastIndex(globPattern, "-")
	reducerID := globPattern[lastDash+1:]
	if err != nil {
		fmt.Printf("reducer globbing intermediate files: %v", err)
		return
	}
	combinedMap := make(map[string][]string)
	for _, mapOutput := range mapOutputs {
		mapFile, err := os.Open(mapOutput)
		defer mapFile.Close()
		if err != nil {
			fmt.Printf("reducer reading intermediate: %v", err)
			return
		}
		decoder := json.NewDecoder(mapFile)
		kv := KeyValue{}
		for decoder.More() {
			// more objects to parse
			err := decoder.Decode(&kv)
			if err != nil {
				fmt.Printf("reducer decoding: %v", err)
				return
			}
			combinedMap[kv.Key] = append(combinedMap[kv.Key], kv.Value)
		}
	}

	outName := fmt.Sprintf("mr-out-%s-", reducerID)
	currentDir, dirErr := os.Getwd()
	if dirErr != nil {
		fmt.Printf("reducer getting current dir: %v\nb", dirErr)
		return
	}
	outFile, err := ioutil.TempFile(currentDir, outName)
	defer outFile.Close()

	if err != nil {
		fmt.Printf("reducer creating output file: %v", err)
		return
	}

	for key, values := range combinedMap {
		fmt.Fprintf(outFile, "%s %s\n", key, reducef(key, values))
	}
	tempName := outFile.Name()
	permName := tempName[:strings.LastIndex(tempName, "-")]
	os.Rename(tempName, permName)
}

// call sends an RPC request to the master and returns the error
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("worker dialing master. Master done. Exiting:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
