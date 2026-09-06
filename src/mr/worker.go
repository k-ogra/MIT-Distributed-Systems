package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// Loop indef while 
	for {
		args := RequestTask{}
		reply := TaskReply{}

		ok := call("Coordinator.GiveTask", &args, &reply)
		if !ok {
			// coordinator gone — assume job is done, exit
			return
		}

		switch reply.M_taskType {
			case TaskWait:
				time.Sleep(5 * time.Second)
			case TaskMap: 
				filename := reply.M_filename
				// intermediate := []KeyValue{}

				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				// sort.Sort(ByKey(intermediate))

				tmpFiles := make([]*os.File, reply.M_nReduce)
				encoders := make([]*json.Encoder, reply.M_nReduce)
				for i := 0; i < reply.M_nReduce; i++ {
					tmpFiles[i], _ = os.CreateTemp(".", "mr-tmp-*")
					encoders[i] = json.NewEncoder(tmpFiles[i])
				}

				for _, kv := range kva {
					encoders[ihash(kv.Key)%reply.M_nReduce].Encode(&kv)
				}

				for i := 0; i < reply.M_nReduce; i++ {
					tmpFiles[i].Close()
					os.Rename(tmpFiles[i].Name(), fmt.Sprintf("mr-int-%d-%d", reply.M_taskID, i))
				}

				taskCompletionArgs := TaskCompletion{M_taskId: reply.M_taskID, M_taskType: TaskMap}
				// TODO: This is unused could we remove? 
				replyCompletion := TaskCompletion{}
				ok := call("Coordinator.RegisterTaskCompletion", &taskCompletionArgs, &replyCompletion)
				if !ok {
					log.Fatalf("register task compl failed")
				}
				


			case TaskReduce:
				reduceTaskNum := reply.M_taskID
				tmpFile, _ := os.CreateTemp(".", "mr-tmp-" + strconv.Itoa(reduceTaskNum))

				// Find all mapped files ending in Y(reduceNum) 
				matches, err := filepath.Glob("mr-int-*-" + strconv.Itoa(reduceTaskNum))
				if err != nil {
					log.Fatal(err)
				}

				kva := []KeyValue{}
				for _, filename := range matches {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatal(err)
					}
						
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							if err == io.EOF {
								break
							}
							log.Fatalf("decode %v: %v", filename, err)
						}
						kva = append(kva, kv)
					}
					file.Close()
				}

				// call Reduce on each distinct key in kva[],
				// and print the result to mr-out-Y.
				sort.Sort(ByKey(kva))
				i := 0
				for i < len(kva) {
					j := i + 1
					// get however many of curr key K there are 
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					// aggregate count of current key K  
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", reduceTaskNum))


				taskCompletionArgs := TaskCompletion{M_taskId: reply.M_taskID, M_taskType: TaskReduce}
				// TODO: This is unused could we remove? 
				replyCompletion := TaskCompletion{}
				ok := call("Coordinator.RegisterTaskCompletion", &taskCompletionArgs, &replyCompletion)
				if !ok {
					log.Fatalf("register task compl failed")
				}
		}	
	}
}


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
