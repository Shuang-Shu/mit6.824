package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// request type
const (
	REQUEST_FOR_WORK = iota // the first request, when receiving this request, coordinator should put workers' id into mapWorkerIds
)

// worker type
const (
	MAP_WORKER = iota
	REDUCE_WORKER
)

// the id of worker
var uniqueId = 1

type Coordinator struct {
	// Your definitions here.
	mapWorkerIds   []int // ids of map workers
	reduceWokerIds []int // ids of reduce workers
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// handshake request rpc
func (c *Coordinator) Handshake(args *Args, reply *Reply) error {
	reply.WorkerId = uniqueId
	uniqueId++
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c) // Register publishes the receiver's methods in the DefaultServer. Therefore, rpc can call specified method of remote request.
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
	ret := false

	// Your code here.

	return ret
}

// request args
type Args struct {
	RequestType int // request type, defined by const
	WorkerId    int // id of worker, received from coordinator in first request
	WorkerType  int // type of worker, may be MAP or REDUCE
}

// request reply
type Reply struct {
	WorkerId int
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
