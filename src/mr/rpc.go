package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Request struct{
	TaskType 	int 	//0 none 1 Map 2 Reduce 
	TaskId		int		//taskid

}

type Reply struct
{
	TaskType 	int 	//1 Map 2 Reduce 3 wait 4 finished
	TaskId		int		//taskid
	FileName	string	//task address,Map only
	NMap		int
	NReduce		int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
