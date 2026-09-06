package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


type RequestTask struct {
	// gob cannot encode a struct with no exported fields, so carry a
	// placeholder field even though the request needs no real payload.
	M_workerID int
}

type TaskType int
const (
	TaskMap TaskType = iota
	TaskReduce 
	TaskWait
)

type TaskReply struct {
	M_filename string 
	// The total number of reduce tasks
	M_nReduce int
	M_taskID int 
	M_taskType TaskType
}


type TaskCompletion struct {
	M_taskId int 
	M_taskType TaskType 
}




// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
