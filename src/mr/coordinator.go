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
	TaskAvailable TaskState = iota
	TaskInProgress
	TaskDone
)

// Enum for state of the coordinator
type CoordinatorState int
const (
	CoordinatorMapping CoordinatorState = iota
	CoordinatorMapped
	CoordinatorReducing
	CoordinatorDone
)

type MapTask struct {
	m_taskState TaskState
	m_fileName string
}

type Coordinator struct {
	// Represents current state of all map tasks
	m_mapTasks map[int]MapTask
	// Map each reduce task Y to whether it is
	m_reduceTasks map[int]TaskState
	// Lock for the taskState map
	m_taskMapLock    sync.Mutex
	// Number of reduce tasks
	m_nReduce  int
	// State of the Coordinator
	m_state  CoordinatorState
}

func checkTaskCompletion(p_taskId int, c *Coordinator) {
	time.Sleep(10)
	c.m_taskMapLock.Lock()
	defer c.m_taskMapLock.Unlock()

	if c.m_state == CoordinatorReducing && c.m_mapTasks[p_taskId].m_taskState == TaskInProgress {
		task := c.m_mapTasks[p_taskId]
		task.m_taskState = TaskAvailable
		c.m_mapTasks[p_taskId] = task 
	}	
	if c.m_state == CoordinatorMapping && c.m_mapTasks[p_taskId].m_taskState == TaskInProgress {
		c.m_reduceTasks[p_taskId] = TaskAvailable
	}
}

// Your code here -- RPC handlers for the worker to call.

//! gives task back to a worker 
func (c *Coordinator) GiveTask(args *RequestTask, reply *TaskReply) error {
	c.m_taskMapLock.Lock()
	defer c.m_taskMapLock.Unlock()

	reply.m_nReduce = c.m_nReduce
	switch c.m_state {
		case CoordinatorDone:
			return nil
		case CoordinatorMapping:
			reply.m_taskType = TaskMap
			found := false
			for k, v := range c.m_mapTasks {
				if v.m_taskState == TaskAvailable {
					reply.m_filename = v.m_fileName
					reply.m_taskID = k
					found = true
					v.m_taskState = TaskInProgress
					break 
				}
			}
			if !found {
				reply.m_taskType = TaskWait
			}
		case CoordinatorReducing:
			reply.m_taskType = TaskReduce
			found := false
			for k, v := range c.m_reduceTasks {
				if v == TaskAvailable {
					reply.m_taskID = k
					found = true
					v = TaskInProgress
					break 
				}
			}
			if !found {
				reply.m_taskType = TaskWait
			}
	}

	// Spawn another thread that checks if the given taskID was completed within 10 sec 
	go checkTaskCompletion(reply.m_taskID, c)
	return nil
}

func (c *Coordinator) RegisterTaskCompletion(args *TaskCompletion, reply *TaskCompletion) error {
	c.m_taskMapLock.Lock()
	defer c.m_taskMapLock.Unlock()
	
	if int(args.m_taskType) == int(TaskMap) {
		// handle map task completion
		task := c.m_mapTasks[args.m_taskId]
		task.m_taskState = TaskDone
		c.m_mapTasks[args.m_taskId] = task
	} else {
		c.m_reduceTasks[args.m_taskId] = TaskDone
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
	return c.m_state == CoordinatorDone  
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.m_nReduce = nReduce
	c.m_state = CoordinatorMapping
	
	for i, v := range files {
		mapTask := MapTask{m_taskState: TaskAvailable, m_fileName: v}
		c.m_mapTasks[i] = mapTask
	}

	for i := 0; i < c.m_nReduce; i++ { 
		c.m_reduceTasks[i] = TaskAvailable
	}		

	c.server()
	return &c
}
