package mr

import (
	"fmt"
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
	files			[]string	//files list
	nMap			int			//tot Map nums
	nReduce			int			//tot Reduce nums
	edMap			int			//finished Map nums
	statusMap		[]int		//Task Map status 0 undo 1 doing 2 done
	edReduce		int			//finished Reduce nums
	statusReduce	[]int		//Task Reduce status
	mu				sync.Mutex	//mutex
}

// Your code here -- RPC handlers for the worker to call.


func (c *Coordinator) Allocate(request *Request,reply *Reply)error {
	switch request.TaskType{
		case 1 :{
			c.mu.Lock()
			if c.statusMap[request.TaskId]==1{
				c.statusMap[request.TaskId]=2
				fmt.Printf("MapTask%v:over\n",request.TaskId)
				c.edMap++
			}
			c.mu.Unlock()
		}
		case 2:{
			c.mu.Lock()
			if c.statusReduce[request.TaskId]==1{
				c.statusReduce[request.TaskId]=2
				fmt.Printf("ReduceTask%v:over\n",request.TaskId)
				c.edReduce++
			}
			c.mu.Unlock()
		}
		default:{}
	}

	request.TaskType=0
	c.mu.Lock()
	fmt.Printf("%v/%v,%v/%v\n",c.edMap,c.nMap,c.edReduce,c.nReduce)
	if c.edMap<c.nMap{
		taskId:=-1
		for i :=0;i<c.nMap;i++{
			if c.statusMap[i]==0 {
				taskId=i
				break
			}
		}
		if taskId==-1{
			reply.TaskType=3
			reply.FileName="Waiting Stage"
			c.mu.Unlock()
		}else{
			reply.TaskType=1
			reply.TaskId=taskId
			reply.FileName=c.files[taskId]
			reply.NMap=c.nMap
			reply.NReduce=c.nReduce	
			fmt.Printf("MapTask:%v allocated\n",reply.TaskId)
			c.statusMap[taskId]=1
			c.mu.Unlock()
			
			go func(){
				time.Sleep(time.Duration(10)*time.Second)
				c.mu.Lock()
				if c.statusMap[taskId]==1{
					c.statusMap[taskId]=0
				}
				c.mu.Unlock()
			}()
		}
	}else if c.edReduce<c.nReduce{
		taskId:=-1
		for i :=0;i<c.nReduce;i++{
			if c.statusReduce[i]==0 {
				taskId=i
				break
			}
		}
		if taskId==-1{
			reply.TaskType=3
			reply.FileName="Waiting Stage"
			c.mu.Unlock()
		}else{
			reply.TaskType=2
			reply.TaskId=taskId
			reply.FileName="Reduce Stage"
			reply.NMap=c.nMap
			reply.NReduce=c.nReduce	
			c.statusReduce[taskId]=1
			fmt.Printf("ReduceTask:%v allocated\n",reply.TaskId)
			c.mu.Unlock()
			
			go func(){
				time.Sleep(time.Duration(10)*time.Second)
				c.mu.Lock()
				if c.statusReduce[taskId]==1{
					c.statusReduce[taskId]=0
				}
				c.mu.Unlock()
			}()
		}
	}else{
		reply.TaskType=4
		c.mu.Unlock()
	}
		
	// fmt.Println(&reply,reply.TaskType,reply.Taskid,reply.Filename,reply.NReduce,reply.NMap)
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
	ret := c.nReduce==c.edReduce
	c.mu.Unlock()
	return ret
}
// func (c *Coordinator) Done() bool {
// 	// Your code here.
	
// 	return true
// }

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{files,
		len(files),
		nReduce,
		0,
		make([]int,len(files)),
		0,
		make([]int,nReduce),
		sync.Mutex{}}
	
	c.server()
	return &c
}
