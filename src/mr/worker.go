package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const ()

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	// 1. Send a work request RPC to the coordinator, then get the workerId
	reply := callHandshake(MAP_WORK)
	if reply.Term == -1 {
		fmt.Printf("call failed!\n")
	} else {
		fmt.Println("ok", reply.WorkerId)
	}
	// 2. Start a keepAlive goroutine
	go callKeepAlive(reply.WorkerId, MAP_WORK, reply.Term)
	// 3. get work
	// 3.1 map work
	if reply.WorkType == MAP_WORK {
		intermediate := []KeyValue{}
		file, err := os.Open(reply.FileName)
		if err != nil {
			fmt.Println("cannot open file")
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FileName)
		}
		file.Close()

		kva := mapf(reply.FileName, string(content))
		intermediate = append(intermediate, kva...)
		outJson, err := json.Marshal(intermediate)
		if err != nil {
			fmt.Println("cannot get json of intermediate")
		}
		/*
			save intermediate result to a file
			its name format is 'mr-X-Y'
			which X is ths task number, Y is reduce task id
			Y is caculated by ihash fucntion
		*/
		strI := strconv.Itoa(reply.WorkerId)
		y := ihash(strI) % reply.ReduceNumber
		strY := strconv.Itoa(y)
		fileName := "mr-" + strI + "-" + strY
		intermediateFile, err := os.Create(fileName)
		if err != nil {
			fmt.Println("cannot create file")
		}
		intermediateFile.Write(outJson)
	} else if reply.WorkType == REDUCE_WORK {
		fmt.Println("not finished")
	}

}

// handshake request
func callHandshake(workerType int) Reply {
	args := Args{}
	args.RequestType = REQUEST_FOR_WORK
	args.WorkerType = workerType
	reply := Reply{}
	ok := call("Coordinator.Handshake", &args, &reply)
	if ok {
		return reply
	} else {
		return Reply{-1, -1, "", -1, -1}
	}
}

// keepAlive request
func callKeepAlive(workerId int, workerType int, term int) {
	for {
		time.Sleep(time.Second) // call keepAlive rpc per second
		args, reply := Args{}, Reply{}
		args.Term = term
		term++
		args.WorkerId = workerId
		args.WorkerType = workerType
		call("Coordinator.KeepAlive", &args, &reply)
		term = reply.Term // synchronize termId by reply
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
