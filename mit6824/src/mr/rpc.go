package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

// TaskType indicates to workers whether to perform
// a map job or a reduce job
type TaskType int

// TaskType == 0 for a Map job and == 1 for a Reduce job
const (
	Map TaskType = iota
	Reduce
)

// ErrWait is error returned by Master RPC if all jobs currently assigned
// to other workers but not necessarily completed.
var ErrWait = errors.New("all jobs currently assigned. Wait")

// ErrDone is error returned by Master RPC if all tasks are completed
var ErrDone = errors.New("all tasks done")

// TaskResponse is the RPC response sent by the master
// to workers upon their request for a task to work on
type TaskResponse struct {
	TaskType
	Filename string
	NReduce  int
	TaskID   int
}

// TaskArgs are the args sent by a worker with an RPC
// request to the master when asking for a task to work on.
// Currently no info is sent.
type TaskArgs struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
