package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// TaskType is the type of task requested by a worker
// to the master via RPC. Either Map == 0 or Reduce == 1
type TaskType int

// TaskTypes that can be requested over RPC to Master
const (
	Map TaskType = iota
	Reduce
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
