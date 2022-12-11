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
		var prefix string
		for {
			reply = callRequest(&args, &reply)
			nilCount++
			if reply.Work.WorkId != 0 {
				break
			}
			fmt.Println("failed to get a work")
			if nilCount > 50 {
				fmt.Println("no new work, exit")
				return
			}
			time.Sleep(10 * time.Second)
		}
		// working
		if reply.Work.WorkType == MAP_WORK {
			// map work
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
			/*
				save intermediate result to a file
				its name format is 'mr-Y-X'
				X is term of task
				Y is reduce task id
				Y is caculated by ihash fucntion
			*/
			strX := strconv.Itoa(reply.Work.WorkId)
			mapedKv := make([][]KeyValue, reply.ReduceNumber)
			for _, kv := range kva {
				y := ihash(kv.Key) % reply.ReduceNumber
				if mapedKv[y] == nil {
					mapedKv[y] = make([]KeyValue, 0)
				}
				mapedKv[y] = append(mapedKv[y], kv)
			}
			// save mapped intermediate
			for i := 0; i < reply.ReduceNumber; i++ {
				if mapedKv[i] == nil {
					continue
				}
				prefix = "mr-" + strconv.Itoa(i)
				resultname = prefix + "-" + strX
				file, _ := os.Create(resultname)
				outJson, _ := json.Marshal(mapedKv[i])
				file.Write(outJson)
				file.Close()
			}
		} else if reply.Work.WorkType == REDUCE_WORK {
			// reduce work
			var targetFile *os.File
			var err error
			for {
				strs := strings.Split(reply.Work.Filename, "-")
				targetFilename := strs[0] + "-" + strs[1] + "-" + strconv.Itoa(reply.Work.WorkId) + "-out"
				args.Work = reply.Work
				args.Work.Filename = targetFilename
				targetFile, err = os.Open(targetFilename)
				if err != nil {
					targetFile, err = os.Create(targetFilename)
					if err != nil {
						time.Sleep(time.Second)
						continue
					}
				}
				break
			}
			// find all files in current directory
			files, err := os.Open(".")
			if err != nil {
				panic(err)
			}
			defer files.Close()

			// read file infos
			fileInfos, err := files.Readdir(-1)
			if err != nil {
				panic(err)
			}

			intermediate := []KeyValue{}
			syscall.Flock(int(targetFile.Fd()), syscall.LOCK_EX) // request for a ex-lock
			for _, fileInfo := range fileInfos {
				if strings.HasPrefix(fileInfo.Name(), reply.Work.Filename) && !strings.HasSuffix(fileInfo.Name(), "-out") {
					fileWorkId, _ := strconv.Atoi(strings.Split(fileInfo.Name(), "-")[2])
					ok := false
					for _, validId := range reply.ValidWorkIdSet {
						if validId == fileWorkId {
							ok = true
							break
						}
					}
					if ok {
						file, _ := os.Open(fileInfo.Name())
						syscall.Flock(int(file.Fd()), syscall.LOCK_SH) // request for a s-lock
						content, _ := ioutil.ReadAll(file)
						partIntermediate := []KeyValue{}
						json.Unmarshal(content, &partIntermediate)
						intermediate = append(intermediate, partIntermediate...)
						file.Close()
					}
				}
			}
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
			targetFile.Close()
		}
		// finished
		args.RequestType = REPORT
		if args.Work.WorkType == MAP_WORK {
			args.Work = reply.Work
		}

		callRequest(&args, &Reply{})
		time.Sleep(time.Second)
	}
}

// request
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
