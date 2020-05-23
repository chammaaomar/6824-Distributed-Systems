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

// Master distributes tasks to workers and keeps tracks
// of completed tasks and dead workers
type Master struct {
	AssignedMaps   map[string]jobStatus
	AssignedReduce map[string]jobStatus
	mapMux         sync.Mutex
	countMux       sync.Mutex
	MapsRemain     int
	MapID          int
	ReduceID       int
	ReduceRemain   int
}

// assignMap assigns a worker to work on a map task
// where the chunk name is given by file
func (m *Master) assignMap(file string, reply *TaskResponse) {
	m.AssignedMaps[file] = inProgress
	reply.TaskID = m.MapID
	reply.TaskType = Map
	reply.Filename = file
	// since no reduce jobs execute before all
	// map jobs are done, this is just the number
	// of reduce jobs passed by the user
	reply.NReduce = m.ReduceRemain
	m.MapID++
}

// assignReduce assigns a worker to work on a reduce task
// where the chunk name is given by file
func (m *Master) assignReduce(file string, reply *TaskResponse) {
	m.AssignedReduce[file] = inProgress
	reply.TaskID = m.ReduceID
	reply.TaskType = Reduce
	reply.Filename = file
	m.ReduceID++
}

// checkMapHealth is launched as a go-routine by master
// to check if the assigned worker is done within 10 seconds
// of starting a map task
func (m *Master) checkMapHealth(file string) {
	time.Sleep(10 * time.Second)
	if m.AssignedMaps[file] == completed {
		return
	}
	// assume worker is dead; re-assign process
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	m.AssignedMaps[file] = free
}

// checkReduceHealth is launched as a go-routine by master
// to check if the assigned worker is done within 10 seconds
// of starting a reduce task
func (m *Master) checkReduceHealth(file string) {
	time.Sleep(10 * time.Second)
	if m.AssignedReduce[file] == completed {
		return
	}
	// assume worker is dead; re-assign process
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	m.AssignedReduce[file] = free
}

// RequestTask is an RPC via which workers can ask for jobs.
// It sends back either a Map job, or a Reduce job, or tells the
// worker to wait if all jobs are currently assigned but not
// completed. An assigned job may be re-assigned if the worker
// dies or takes too long.
func (m *Master) RequestTask(TaskArgs, reply *TaskResponse) error {

	// reduce jobs can only run AFTER all map jobs have finished
	// once reduce jobs have started, there are no map jobs remaining
	// so we can safely lock everything here, because they cannot
	// be used simultaneously
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	if m.MapsRemain > 0 {
		// try to assign a map task that isn't assigned to another
		// worker
		for file, status := range m.AssignedMaps {
			if status == free {
				m.assignMap(file, reply)
				go m.checkMapHealth(file)
				return nil
			}
			// all map tasks currently assigned, but not necessarily completed
		}
		return ErrWait
	}
	// no map tasks remaining, ready to reduce
	if m.ReduceRemain > 0 {
		for file, status := range m.AssignedReduce {
			if status == free {
				m.assignReduce(file, reply)
				go m.checkReduceHealth(file)
				// assigned task to worker
				return nil
			}
		}
		return ErrWait
	}
	return ErrDone
}

// NotifyDone is a method to be called by workers via RPC to let
// the master know they finished their task. If not called within
// ten seconds of starting the job, the master will assume the worker
// is dead.
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
	if m.ReduceRemain == 0 {
		go func() {
			fmt.Println("master: All tasks completed. Exiting...")
			time.Sleep(time.Second)
			os.Exit(0)
		}()
	}

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

// MakeMaster creates a master with a mapper for each
// input file in files, and nReduce outputs
func MakeMaster(files []string, nReduce int) *Master {
	assignedTasks := make(map[string]jobStatus)
	assignedReduce := make(map[string]jobStatus)
	// currently no tasks are assigned to workers
	// initialize
	for _, f := range files {
		assignedTasks[f] = free
	}
	for i := 0; i < nReduce; i++ {
		assignedReduce[fmt.Sprintf("mr-worker-[0-9]*-%d", i)] = free
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
