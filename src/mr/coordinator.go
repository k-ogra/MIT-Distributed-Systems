package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Enum for state of a task
type TaskState int
const (
	// The task is available
	TaskAvailable TaskState = iota
	// The task is in progress 
	TaskInProgress
	// The task is done 
	TaskDone
)

// Enum for state of the coordinator
type CoordinatorState int
const (
	// Coordinator still has map tasks to handout 
	CoordinatorMapping CoordinatorState = iota
	// Workers have finished all map tasks
	CoordinatorMapped
	// Coordinator still has reduce tasks to handout 
	CoordinatorReducing
	// All map and reduce tasks are done 
	CoordinatorDone
)

type MapTask struct {
	// Current state of a given map task
	M_taskState TaskState
	// File associated with the map task 
	M_fileName string
}

type Coordinator struct {
	// Represents current state of all map tasks
	M_mapTasks map[int]MapTask
	// Map each reduce task Y to it's state 
	M_reduceTasks map[int]TaskState
	// Lock for the taskState map
	M_taskMapLock    sync.Mutex
	// Number of reduce tasks
	M_nReduce  int
	// State of the Coordinator
	M_state  CoordinatorState
}

//! Runs in its own thread to check whether the task was finished within 10 sec 
func checkTaskCompletion(p_taskId int, c *Coordinator) {
	time.Sleep(10 * time.Second)
	c.M_taskMapLock.Lock()
	defer c.M_taskMapLock.Unlock()

	if c.M_state == CoordinatorMapping && c.M_mapTasks[p_taskId].M_taskState == TaskInProgress {
		task := c.M_mapTasks[p_taskId]
		task.M_taskState = TaskAvailable
		c.M_mapTasks[p_taskId] = task 
	}	
	if c.M_state == CoordinatorReducing && c.M_reduceTasks[p_taskId] == TaskInProgress {
		c.M_reduceTasks[p_taskId] = TaskAvailable
	}
}

// Your code here -- RPC handlers for the worker to call.

//! gives task back to a worker 
func (c *Coordinator) GiveTask(args *RequestTask, reply *TaskReply) error {
	c.M_taskMapLock.Lock()
	defer c.M_taskMapLock.Unlock()

	reply.M_nReduce = c.M_nReduce
	switch c.M_state {
		case CoordinatorDone:
			// TaskMap is the zero value of TaskType, so an unset reply would
			// look like a map task for the empty filename.
			reply.M_taskType = TaskWait
			return nil
		case CoordinatorMapping:
			reply.M_taskType = TaskMap
			isTaskAvailable := false
			for k, v := range c.M_mapTasks {
				if v.M_taskState == TaskAvailable {
					reply.M_filename = v.M_fileName
					reply.M_taskID = k
					isTaskAvailable = true

					task := c.M_mapTasks[k]
					task.M_taskState = TaskInProgress
					c.M_mapTasks[k] = task
					break 
				}
			}
			if !isTaskAvailable {
				reply.M_taskType = TaskWait
			}
		case CoordinatorReducing:
			reply.M_taskType = TaskReduce
			isTaskAvailable := false
			for k, v := range c.M_reduceTasks {
				if v == TaskAvailable {
					reply.M_taskID = k
					isTaskAvailable = true
					c.M_reduceTasks[k] = TaskInProgress
					break 
				}
			}
			if !isTaskAvailable {
				reply.M_taskType = TaskWait
			}
	}

	// Spawn another thread that checks if the given taskID was completed within 10 sec 
	go checkTaskCompletion(reply.M_taskID, c)
	return nil
}

// Lets workers modify the corrdinater's map to mark map/reduce tasks as done 
func (c *Coordinator) RegisterTaskCompletion(args *TaskCompletion, reply *TaskCompletion) error {
	c.M_taskMapLock.Lock()
	defer c.M_taskMapLock.Unlock()
	
	if args.M_taskType == TaskMap {
		// handle map task completion
		task := c.M_mapTasks[args.M_taskId]
		task.M_taskState = TaskDone
		c.M_mapTasks[args.M_taskId] = task
		// change coordinator's state to reducing if all map tasks are done 
		for _, v := range c.M_mapTasks {
			if v.M_taskState != TaskDone {
				return nil
			}
		}
		c.M_state = CoordinatorReducing
	} else {
		c.M_reduceTasks[args.M_taskId] = TaskDone
		for _, v := range c.M_reduceTasks {
			if v != TaskDone {
				return nil
			}
		}
		c.M_state = CoordinatorDone
	}
	return nil
}




//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.M_taskMapLock.Lock()
	defer c.M_taskMapLock.Unlock()
	return c.M_state == CoordinatorDone  
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.M_nReduce = nReduce
	c.M_state = CoordinatorMapping
	c.M_mapTasks = make(map[int]MapTask)
	c.M_reduceTasks = make(map[int]TaskState)

	for i, v := range files {
		mapTask := MapTask{M_taskState: TaskAvailable, M_fileName: v}
		c.M_mapTasks[i] = mapTask
	}

	for i := 0; i < c.M_nReduce; i++ { 
		c.M_reduceTasks[i] = TaskAvailable
	}		

	c.server()
	return &c
}
