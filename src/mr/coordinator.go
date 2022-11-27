package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
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

// current term of coordinator
var currentTerm int = 0

// Mutex
var mutex sync.Mutex = sync.Mutex{}

type Coordinator struct {
	// Your definitions here.
	mapWorkers   map[int]int // map from mapWorkerId to term
	reduceWokers map[int]int // map from reduceWorkerId to term
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
	reply.TermId = currentTerm
	if args.WorkerType == MAP_WORKER {
		c.mapWorkers[uniqueId] = currentTerm
	} else if args.WorkerType == REDUCE_WORKER {
		c.reduceWokers[uniqueId] = currentTerm
	}
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
	TermId      int // current term of worker
}

// request reply
type Reply struct {
	WorkerId int
	TermId   int // current term of coordinator
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//
	c.mapWorkers = make(map[int]int)
	c.reduceWokers = make(map[int]int)
	go c.checkAlive() // start keepAlive goroutine
	c.server()
	return &c
}

// keep alive function, in a single goroutine
func (c *Coordinator) checkAlive() {
	for {
		time.Sleep(time.Second) // check worker alive status per second
		currentTerm++           // increase current term
		mutex.Lock()
		// check map workers
		for k, v := range c.mapWorkers {
			if currentTerm-v > 10 {
				fmt.Println("worker:" + strconv.Itoa(k) + " die")
				delete(c.mapWorkers, k)
			}
		}
		// check reduce workers
		for k, v := range c.reduceWokers {
			if currentTerm-v > 10 {
				delete(c.reduceWokers, k)
			}
		}
		mutex.Unlock()
	}
}

// keep alive rpc
// map has race problem on c
func (c *Coordinator) KeepAlive(args *Args, reply *Reply) error {
	mutex.Lock()
	if args.WorkerType == MAP_WORKER {
		c.mapWorkers[args.WorkerId] = currentTerm
	} else {
		c.reduceWokers[args.WorkerId] = currentTerm
	}
	mutex.Unlock()
	fmt.Println("worker:" + strconv.Itoa(args.WorkerId) + " keep alive")
	return nil
}
