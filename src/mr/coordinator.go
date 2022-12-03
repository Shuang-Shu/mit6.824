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

// work type
const (
	MAP_WORK = iota
	REDUCE_WORK
	NIL_WORK
)

// request type
const (
	REQUEST = iota
	REPORT
)

// current term of coordinator
var currentTerm int = 0

// reduce task number
var reduceNumber int

// Mutex
var mutex sync.Mutex = sync.Mutex{}

type Work struct {
	WorkType int    // work type, can be map or reduce
	Filename string // name of file that to be processed
}

type Coordinator struct {
	// Your definitions here.
	leftWorks     map[Work]int // works to be done (actually is a set, int value is unused)
	doingWorks    map[Work]int // works is doing, int refer to term that the work starts.
	finishedWorks map[Work]int // works have been done (actually is a set, int value is unused)
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
func (c *Coordinator) Request(args *Args, reply *Reply) error {
	mutex.Lock()
	defer mutex.Unlock()
	if args.RequestType == REQUEST {
		if len(c.leftWorks) == 0 {
			reply.Work.WorkType = NIL_WORK
			return nil
		}
		newWork := c.assignWork()
		// modify data
		delete(c.leftWorks, newWork)
		c.doingWorks[newWork] = currentTerm
		// modify reply
		reply.Work = newWork
		reply.ReduceNumber = reduceNumber
	} else {
		_, ok := c.doingWorks[args.Work]
		if ok {
			c.finishedWorks[args.Work] = 0
			delete(c.doingWorks, args.Work)
			if args.Work.WorkType == MAP_WORK {
				reduceWork := Work{REDUCE_WORK, args.IntermediateName}
				c.leftWorks[reduceWork] = 0
			}
		}
		reply.Work.WorkType = NIL_WORK
	}
	return nil
}

// asign a work
func (c *Coordinator) assignWork() Work {
	newWork := Work{}
	for w := range c.leftWorks {
		newWork = w
	}
	return newWork
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
	mutex.Lock()
	defer mutex.Unlock()
	if len(c.doingWorks)+len(c.leftWorks) == 0 {
		ret = true
	}
	return ret
}

// request args
type Args struct {
	Work             Work   // work to be reported
	RequestType      int    // request type, may be request or report
	IntermediateName string // intermediate file name
}

// request reply
type Reply struct {
	Work         Work // work to be assigned
	ReduceNumber int  // reduce tasks number
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//
	reduceNumber = nReduce
	c.doingWorks = make(map[Work]int)
	c.finishedWorks = make(map[Work]int)
	c.leftWorks = make(map[Work]int)
	for _, fn := range files {
		work := Work{MAP_WORK, fn}
		c.leftWorks[work] = 0
	}
	go timeFlies()
	go c.killLazy()
	c.server()
	return &c
}

// increase term per sec
func timeFlies() {
	for {
		time.Sleep(time.Second)
		mutex.Lock()
		currentTerm = currentTerm + 1
		mutex.Unlock()
	}
}

// clear the dead doing work
func (c *Coordinator) killLazy() {
	for {
		time.Sleep(5 * time.Second)
		mutex.Lock()
		// delete element in iteration is safe in golang
		for work, term := range c.doingWorks {
			if currentTerm-term > 10 {
				delete(c.doingWorks, work)
				c.leftWorks[work] = 0
			}
		}
		mutex.Unlock()
	}
}
