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

// sent when worker requests a task 
type RequestTask struct {
	// placeholder
	M_workerID int
}

// Type of task
type TaskType int
const (
	// Map task
	TaskMap TaskType = iota
	// Reduce task
	TaskReduce
	// Worker should wait until receiving another task 
	TaskWait
)

// reply for a task request 
type TaskReply struct {
	// Filename 
	M_filename string 
	// The total number of reduce tasks
	M_nReduce int
	// Id for a task
	M_taskID int 
	// Type of task 
	M_taskType TaskType
}

// tell coordinator that a given task has finished
type TaskCompletion struct {
	// the task id
	M_taskId int 
	// The task type 
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
