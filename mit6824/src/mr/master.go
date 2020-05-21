package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// the Master handles RPCs concurrenctly
	AssignedMaps   map[string]bool
	AssignedReduce map[string]bool
	mux            sync.Mutex
	MapsRemain     int
	MapID          int
	ReduceID       int
	ReduceRemain   int
}

// RequestTask is an RPC via which workers can ask for jobs
// it send back either a Map job, or a Reduce job, or tells the
// worker to wait if all jobs are currently assigned but not
// completed. The worker waits in case one of the workers fails.
func (m *Master) RequestTask(TaskArgs, reply *TaskResponse) error {
	if m.MapsRemain > 0 {
		// try to assign a map task that isn't assigned to another
		// worker
		for file, assigned := range m.AssignedMaps {
			if !assigned {
				reply.TaskID = m.MapID
				reply.TaskType = Map
				reply.Filename = file
				// since no reduce jobs execute before all
				// map jobs are done, this is just the number
				// of reduce jobs passed by the user
				reply.NReduce = m.ReduceRemain
				m.MapID++
				return nil
			}
		}
		// all map tasks currently assigned, but not necessarily completed
		return ErrWait
	}
	// no map tasks remaining, ready to reduce
	if m.ReduceRemain > 0 {
		for file, assigned := range m.AssignedReduce {
			if !assigned {
				reply.TaskID = m.ReduceID
				reply.TaskType = Reduce
				reply.Filename = file
				m.ReduceID++
				return nil
			}
		}
		return ErrWait
	}
	return ErrDone
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
		AssignedMaps:   assignedTasks,
		AssignedReduce: map[string]bool{},
		ReduceRemain:   nReduce,
		MapsRemain:     len(files),
	}
	// master does not have to start the workers
	m.server()
	return &m
}
