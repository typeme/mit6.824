package mr

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskIdle = iota
	TaskWorking
	TaskCommit
)

type Master struct {
	// Your definitions here.
	files   []string
	nReduce int

	mapTasks     []int
	reducesTasks []int

	mapCount     int
	workerCommit map[string]int
	allCommitted bool

	timeout time.Duration

	mu        sync.RWMutex
	workerNum int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func genWorkerId() (uuid string) {
	unix32bits := uint32(time.Now().UTC().Unix())
	buff := make([]byte, 12)
	numRead, err := rand.Read(buff)

	if numRead != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x\n", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])

}

func (m *Master) RegWorker(arg *RegisterReply, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerNum++
	reply.workerId = genWorkerId()
	return nil
}

func (m *Master) Work(args *WorkArgs, reply *WorkReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// TODO: dispatch map work

	// TODO: dispatch reduce work

	// TODO: check if all work have committed

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:        files,
		nReduce:      nReduce,
		mapTasks:     make([]int, len(files)),
		reducesTasks: make([]int, nReduce),
		allCommitted: false,
		timeout:      10 * time.Millisecond,
		workerNum:    0,
	}

	// Your code here.
	log.Println("[init] with:", files, nReduce)

	m.server()
	return &m
}
