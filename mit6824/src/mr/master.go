package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// the Master handles RPCs concurrenctly
	AssignedTasks   map[string]bool
	mux             sync.Mutex
	NReducePending  int
	NMappersPending int
}

// Your code here -- RPC handlers for the worker to call.

// RequestTask is an RPC via which workers can ask for Map jobs
// (taskType == 0) or Reduce jobs (taskType == 1) and receive
// a string pointer holding the filename to work on
func (m *Master) RequestTask(taskType TaskType, reply *string) error {
	if taskType == Map {
		m.mux.Lock()
		defer m.mux.Unlock()
		for filename, assigned := range m.AssignedTasks {
			if !assigned {
				*reply = filename
				m.AssignedTasks[filename] = true
				return nil
			}
		}
	}
	if taskType == Reduce {
		// not implemented yet
		return errors.New("Reduce not yet implemented")
	}

	return errors.New("Allowed taskType values are 0 for Map and 1 for Reduce")
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	assignedTasks := make(map[string]bool)

	// currently no tasks are assigned to workers
	for _, f := range files {
		assignedTasks[f] = false
	}
	m := Master{
		AssignedTasks:   assignedTasks,
		NReducePending:  nReduce,
		NMappersPending: len(files),
	}
	// master does not have to start the workers
	m.server()
	return &m
}
