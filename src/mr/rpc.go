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
type RegisterArgs struct {
}

type RegisterReply struct {
	WorkId string
}

type WorkArgs struct {
	WorkId string
}

type WorkReply struct {
	IsFinished bool

	TaskId     int
	FileName   string
	TaskName   string
	BucketName int
}

type CommitArgs struct {
	WorkerId string
	TaskId   int
	TaskName string
}

type CommitReply struct {
	IsOk bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
