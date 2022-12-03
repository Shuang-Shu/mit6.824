package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
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

// for sorting
type ByKey []KeyValue

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
	for {
		// ready
		args := Args{}
		reply := Reply{}
		nilCount := 0
		args.RequestType = REQUEST
		var resultname string
		for {
			time.Sleep(time.Second)
			reply = callRequest(&args, &reply)
			nilCount++
			if reply.Work.WorkType != NIL_WORK {
				break
			}
			if nilCount > 10 {
				println("no new work, exit")
				return
			}
		}
		// working
		if reply.Work.WorkType == MAP_WORK {
			intermediate := []KeyValue{}
			file, err := os.Open(reply.Work.Filename)
			if err != nil {
				fmt.Println("cannot open file")
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Work.Filename)
			}
			file.Close()

			kva := mapf(reply.Work.Filename, string(content))
			intermediate = append(intermediate, kva...)
			outJson, err := json.Marshal(intermediate)
			if err != nil {
				fmt.Println("cannot get json of intermediate")
			}
			/*
				save intermediate result to a file
				its name format is 'mr-Y-intermediate'
				Y is reduce task id
				Y is caculated by ihash fucntion
			*/
			y := ihash(reply.Work.Filename) % reply.ReduceNumber
			strY := strconv.Itoa(y)
			resultname = "mr-" + strY + "-intermediate"
			intermediateFile, err := os.Create(resultname)
			if err != nil {
				fmt.Println("cannot create file")
			}
			intermediateFile.Write(outJson)
			intermediateFile.Close()
		} else {
			var targetFile *os.File
			var err error
			for {
				time.Sleep(time.Second)
				strs := strings.Split(reply.Work.Filename, "-")
				targetFilename := strs[0] + "-out-" + strs[1]
				targetFile, err = os.Open(targetFilename)
				if err != nil {
					targetFile, err = os.Create(targetFilename)
					if err != nil {
						continue
					}
				}
				break
			}
			file, _ := os.Open(reply.Work.Filename)
			syscall.Flock(int(targetFile.Fd()), syscall.LOCK_EX) // request for a ex-lock
			syscall.Flock(int(file.Fd()), syscall.LOCK_SH)       // request for a s-lock
			content, _ := ioutil.ReadAll(file)
			var intermediate []KeyValue
			json.Unmarshal(content, &intermediate)
			sort.Sort(ByKey(intermediate))
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(targetFile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

			file.Close()
			targetFile.Close()
		}
		// finished
		args.RequestType = REPORT
		args.Work = reply.Work
		args.IntermediateName = resultname
		callRequest(&args, &Reply{})
	}
}

// handshake request
func callRequest(args *Args, reply *Reply) Reply {
	ok := call("Coordinator.Request", args, reply)
	if ok {
		return *reply
	} else {
		return Reply{}
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
