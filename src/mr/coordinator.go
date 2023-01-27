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

type Coordinator struct {
	// Your definitions here.
	files        []string   //files list
	nMap         int        //tot Map nums
	nReduce      int        //tot Reduce nums
	edMap        int        //finished Map nums
	statusMap    []int      //Task Map status 0 undo 1 doing 2 done
	edReduce     int        //finished Reduce nums
	statusReduce []int      //Task Reduce status
	mu           sync.Mutex //mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Allocate(request *Request, reply *Reply) error {
	//process request information
	c.mu.Lock()
	defer c.mu.Unlock()
	switch request.TaskType {
	case 1:
		if c.statusMap[request.TaskId] == 1 {
			c.statusMap[request.TaskId] = 2
			c.edMap++
		}

	case 2:
		if c.statusReduce[request.TaskId] == 1 {
			c.statusReduce[request.TaskId] = 2
			c.edReduce++
		}
	}
	request.TaskType = 0

	// allocate task
	if c.edMap < c.nMap {
		// find undo task's id
		taskId := -1
		for i := 0; i < c.nMap; i++ {
			if c.statusMap[i] == 0 {
				taskId = i
				break
			}
		}
		if taskId == -1 {
			reply.TaskType, reply.FileName =
				3, "Waiting Stage"

		} else {
			reply.TaskType, reply.TaskId, reply.FileName, reply.NMap, reply.NReduce =
				1, taskId, c.files[taskId], c.nMap, c.nReduce
			c.statusMap[taskId] = 1
			/*
				If the task is not completed within 10 seconds,
				the status will be reset and the task will be reassigned
			*/
			go func(taskId int) {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.statusMap[taskId] == 1 {
					c.statusMap[taskId] = 0
				}
				c.mu.Unlock()
			}(taskId)
		}
	} else if c.edReduce < c.nReduce {
		taskId := -1
		for i := 0; i < c.nReduce; i++ {
			if c.statusReduce[i] == 0 {
				taskId = i
				break
			}
		}
		if taskId == -1 {
			reply.TaskType, reply.FileName =
				3, "Waiting Stage"
		} else {
			reply.TaskType, reply.TaskId, reply.FileName, reply.NMap, reply.NReduce =
				2, taskId, "Reduce Stage", c.nMap, c.nReduce
			c.statusReduce[taskId] = 1

			go func(taskId int) {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.statusReduce[taskId] == 1 {
					c.statusReduce[taskId] = 0
				}
				c.mu.Unlock()
			}(taskId)
		}
	} else {
		reply.TaskType = 4
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nReduce == c.edReduce
	// return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:        files,
		nMap:         len(files),
		nReduce:      nReduce,
		edMap:        0,
		statusMap:    make([]int, len(files)),
		edReduce:     0,
		statusReduce: make([]int, nReduce),
	}

	c.server()
	return &c
}
