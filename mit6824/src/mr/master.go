package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type jobStatus int

const (
	free jobStatus = iota
	inProgress
	completed
)

type Master struct {
	// the Master handles RPCs concurrenctly
	AssignedMaps   map[string]jobStatus
	AssignedReduce map[string]jobStatus
	mapMux         sync.Mutex
	countMux       sync.Mutex
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

	// reduce jobs can only run AFTER all map jobs have finished
	// once reduce jobs have start, there are no map jobs remaining
	// so we can safely lock everything here, because they cannot
	// be used simultaneously
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	if m.MapsRemain > 0 {
		// try to assign a map task that isn't assigned to another
		// worker
		for file, status := range m.AssignedMaps {
			if status == free {
				m.AssignedMaps[file] = inProgress
				reply.TaskID = m.MapID
				reply.TaskType = Map
				reply.Filename = file
				// since no reduce jobs execute before all
				// map jobs are done, this is just the number
				// of reduce jobs passed by the user
				reply.NReduce = m.ReduceRemain
				m.MapID++
				go func() {
					time.Sleep(10 * time.Second)
					if m.AssignedMaps[file] == completed {
						return
					}
					// assume worker is dead; re-assign process
					m.mapMux.Lock()
					defer m.mapMux.Unlock()
					m.AssignedMaps[file] = free
					return
				}()
				return nil
			}
		}
		// all map tasks currently assigned, but not necessarily completed
		return ErrWait
	}
	// no map tasks remaining, ready to reduce
	if m.ReduceRemain > 0 {
		for file, status := range m.AssignedReduce {
			if status == free {
				m.AssignedReduce[file] = inProgress
				reply.TaskID = m.ReduceID
				reply.TaskType = Reduce
				reply.Filename = file
				m.ReduceID++
				go func() {
					time.Sleep(10 * time.Second)
					if m.AssignedReduce[file] == completed {
						return
					}
					// assume worker is dead; re-assign process
					m.mapMux.Lock()
					defer m.mapMux.Unlock()
					m.AssignedReduce[file] = free
					return
				}()
				return nil
			}
		}
		return ErrWait
	}
	return ErrDone
}

func (m *Master) NotifyDone(args DoneArgs, reply *DoneResponse) error {
	m.countMux.Lock()
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	defer m.countMux.Unlock()
	if args.TaskType == Map {
		m.MapsRemain--
		m.AssignedMaps[args.Filename] = completed
		return nil
	}
	m.ReduceRemain--
	m.AssignedReduce[args.Filename] = completed
	return nil
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
	assignedTasks := make(map[string]jobStatus)
	assignedReduce := make(map[string]jobStatus)
	// currently no tasks are assigned to workers
	// initialize
	for _, f := range files {
		assignedTasks[f] = free
	}
	for i := 0; i < nReduce; i++ {
		assignedReduce[fmt.Sprintf("mr-[0-9]*-%d", i)] = free
	}
	m := Master{
		AssignedMaps:   assignedTasks,
		AssignedReduce: assignedReduce,
		ReduceRemain:   nReduce,
		MapsRemain:     len(files),
	}

	// master does not have to start the workers
	m.server()
	return &m
}
