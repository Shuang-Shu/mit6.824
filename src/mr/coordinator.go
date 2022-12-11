package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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

// work id
var workId int = 1

// reduce task number
var reduceNumber int

// Mutex
var mutex sync.Mutex = sync.Mutex{}

// save the valid map workId of specified reduce task
var validWorkIdSet []int = make([]int, 0)

// success reduce workId slice
var successReduceId []string = make([]string, 0)

type Work struct {
	WorkId   int    // term that this task is processed
	WorkType int    // work type, can be map or reduce
	Filename string // name of file that to be processed
}

type Coordinator struct {
	// Your definitions here.
	leftMapWorks    map[Work]int // works to be done (actually is a set, int value is unused)
	leftReduceWorks map[Work]int // left reduce works
	doingWorks      map[Work]int // works is doing, int refer to term that the work starts.
	finishedWorks   map[Work]int // works have been done (actually is a set, int value is unused)
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
		// asign a work
		if len(c.leftMapWorks)+len(c.leftReduceWorks) == 0 {
			return nil
		}
		newWork, ok := c.assignWork()
		if !ok {
			fmt.Println(c.doingWorks)
			return nil
		}
		// modify data
		c.deletWork(newWork)
		newWork.WorkId = workId
		c.doingWorks[newWork] = currentTerm
		workId++
		// modify reply
		reply.Work = newWork
		reply.ReduceNumber = reduceNumber
		reply.ValidWorkIdSet = validWorkIdSet
	} else {
		var fullName string
		if args.Work.WorkType == REDUCE_WORK {
			fullName = args.Work.Filename
			nameComponent := strings.Split(args.Work.Filename, "-")
			args.Work.Filename = nameComponent[0] + "-" + nameComponent[1]
		}
		_, ok := c.doingWorks[args.Work]
		if ok {
			if args.Work.WorkType == MAP_WORK {
				validWorkIdSet = append(validWorkIdSet, args.Work.WorkId)
			} else {
				successReduceId = append(successReduceId, fullName)
			}
			c.finishedWorks[args.Work] = 0
			delete(c.doingWorks, args.Work)
		}
	}
	return nil
}

func (c *Coordinator) deletWork(work Work) {
	if work.WorkType == MAP_WORK {
		delete(c.leftMapWorks, work)
	} else if work.WorkType == REDUCE_WORK {
		delete(c.leftReduceWorks, work)
	}
}

// asign a work
func (c *Coordinator) assignWork() (Work, bool) {
	// this is not right. Because thought the c.leftMapWorks is empty, some map tasks may still be running. so this should check running works to ensure that there is no map work is running.
	for w := range c.leftMapWorks {
		return w, true
	}
	// check is there any map work is still running
	for w := range c.doingWorks {
		if w.WorkType == MAP_WORK {
			time.Sleep(500 * time.Millisecond)
			result := Work{}
			result.WorkType = NIL_WORK
			return result, false
		}
	}
	for w := range c.leftReduceWorks {
		return w, true
	}
	return Work{}, false
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
	if len(c.doingWorks)+len(c.leftMapWorks)+len(c.leftReduceWorks) == 0 {
		ret = true
	}
	if ret {
		for _, filename := range successReduceId {
			nameComponent := strings.Split(filename, "-")
			newName := nameComponent[0] + "-out-" + nameComponent[1]
			os.Rename(filename, newName)
		}
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
	Work           Work  // work to be assigned
	ReduceNumber   int   // reduce tasks number
	ValidWorkIdSet []int // valid workId set, only use in reduce work
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//
	fmt.Println(nReduce)
	reduceNumber = nReduce
	c.doingWorks = make(map[Work]int)
	c.finishedWorks = make(map[Work]int)
	c.leftMapWorks = make(map[Work]int)
	c.leftReduceWorks = make(map[Work]int)
	for _, fn := range files {
		if strings.HasSuffix(fn, ".txt") {
			work := Work{currentTerm, MAP_WORK, fn}
			c.leftMapWorks[work] = 0
		}
	}
	for idx := 0; idx < nReduce; idx++ {
		work := Work{currentTerm, REDUCE_WORK, "mr-" + strconv.Itoa(idx)}
		c.leftReduceWorks[work] = 0
	}
	go c.killLazy()
	c.server()
	return &c
}

// clear the dead doing work
func (c *Coordinator) killLazy() {
	for {
		time.Sleep(1 * time.Second)
		mutex.Lock()
		currentTerm++
		// delete element in iteration is safe in golang
		for work, term := range c.doingWorks {
			if currentTerm-term > 10 {
				delete(c.doingWorks, work)
				print("refresh work")
				fmt.Println(work)
				if work.WorkType == MAP_WORK {
					c.leftMapWorks[work] = 0
				} else if work.WorkType == REDUCE_WORK {
					c.leftReduceWorks[work] = 0
				}
			}
		}
		mutex.Unlock()
	}
}
