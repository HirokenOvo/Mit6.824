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
	"time"
)

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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func solveMap(mapf func(string, string) []KeyValue, request *Request, reply *Reply) {
	// load data
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	file.Close()

	// map process
	intermediate := mapf(reply.FileName, string(content))

	// buckets allocate
	buckets := make([][]KeyValue, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		buckets[i] = []KeyValue{}
	}
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % reply.NReduce
		buckets[reduceId] = append(buckets[reduceId], kv)
	}

	// store
	mapId := reply.TaskId
	for i := 0; i < reply.NReduce; i++ {
		targetName := "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(i)
		tmpName, _ := ioutil.TempFile("", targetName+".tmp")
		enc := json.NewEncoder(tmpName)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", tmpName)
			}
		}
		os.Rename(tmpName.Name(), targetName)
		tmpName.Close()
	}
	request.TaskType, request.TaskId =
		1, reply.TaskId
}

func solveReduce(reducef func(string, []string) string, request *Request, reply *Reply) {
	// load data
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	// reduce process and store
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.TaskId)
	ofile, _ := os.Create(oname)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// delete tmpfile
	for i := 0; i < reply.NMap; i = i + 1 {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)
		err := os.Remove(fileName)
		if err != nil {
			log.Fatalf("cannot open delete" + fileName)
		}
	}

	request.TaskType, request.TaskId =
		2, reply.TaskId
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	request := Request{}
	reply := Reply{}
	for {
		ok := call("Coordinator.Allocate", &request, &reply)
		if !ok {
			return
		}
		switch reply.TaskType {
		// Map
		case 1:
			// fmt.Printf("MapTask:%v is running...\n",reply.TaskId)
			solveMap(mapf, &request, &reply)

		// Reduce
		case 2:
			// fmt.Printf("ReduceTask:%v is running...\n",reply.TaskId)
			solveReduce(reducef, &request, &reply)

		// wait
		case 3:
			// fmt.Println("The tasks are all allocated, waiting...")

		// finished
		default:
			// fmt.Println("MapReduce Tasks all over,process exited")
			return
		}
		reply = Reply{}
		time.Sleep(500 * time.Millisecond)

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
