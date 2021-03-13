package mr

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
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

func (m *Master) genWorkerId() (uuid string) {
	//unix32bits := uint32(time.Now().UTC().Unix())
	//buff := make([]byte, 12)
	//numRead, err := rand.Read(buff)
	//
	//if numRead != len(buff) || err != nil {
	//	panic(err)
	//}
	//
	//return fmt.Sprintf("%x-%x-%x-%x-%x-%x\n", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
	return strconv.Itoa(m.workerNum)

}

func (m *Master) RegWorker(arg *RegisterReply, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerNum++
	reply.WorkId = m.genWorkerId()
	return nil
}

func (m *Master) Work(args *WorkArgs, reply *WorkReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// dispatch map work
	for k, v := range m.files {
		if m.mapTasks[k] != TaskIdle {
			continue
		}
		reply.TaskId = k
		reply.FileName = v
		reply.TaskName = "map"
		reply.BucketName = m.nReduce
		reply.IsFinished = false
		m.workerCommit[args.WorkId] = TaskWorking
		m.mapTasks[k] = TaskWorking

		ctx, _ := context.WithTimeout(context.Background(), m.timeout)
		go func() {
			select {
			case <-ctx.Done():
				{
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.workerCommit[args.WorkId] != TaskCommit && m.mapTasks[k] != TaskCommit {
						m.mapTasks[k] = TaskIdle
						log.Println("[Error]:", "worker:", args.WorkId, "map task:", k, "timeout")
					}
				}
			}
		}()
		log.Println("a worker", args.WorkId, "apply a map task:", *reply)
		return nil
	}

	// dispatch reduce work

	for k, v := range m.reducesTasks {
		// judge if all map works have finished
		if m.mapCount != len(m.files) {
			return nil
		}
		if v != TaskIdle {
			continue
		}
		reply.TaskId = k
		reply.FileName = ""
		reply.TaskName = "reduce"
		reply.BucketName = len(m.files)
		reply.IsFinished = false
		m.workerCommit[args.WorkId] = TaskWorking
		m.reducesTasks[k] = TaskWorking

		ctx, _ := context.WithTimeout(context.Background(), m.timeout)
		go func() {
			select {
			case <-ctx.Done():
				{
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.workerCommit[args.WorkId] != TaskCommit && m.reducesTasks[k] != TaskCommit {
						m.reducesTasks[k] = TaskIdle
						log.Println("[Error]:", "worker:", args.WorkId, "reduce task:", k, "timeout")
					}
				}
			}
		}()
		log.Println("a worker", args.WorkId, "apply a reduce task:", *reply)
		return nil
	}

	// check if all work have committed

	for _, v := range m.workerCommit {
		if v == TaskWorking {
			reply.IsFinished = false
			return nil
		}
	}

	reply.IsFinished = true
	return errors.New("no tasks dispatch")

}

func (m *Master) Commit(args *CommitArgs, reply *CommitReply) error {
	log.Println("worker id ", args.WorkerId, "commit a ", args.TaskName, "task id ", args.TaskId)
	m.mu.Lock()
	switch args.TaskName {
	case "map":
		{
			m.mapCount++
			m.mapTasks[args.TaskId] = TaskCommit
			m.workerCommit[args.WorkerId] = TaskCommit

		}
	case "reduce":
		{
			m.reducesTasks[args.TaskId] = TaskCommit
			m.workerCommit[args.WorkerId] = TaskCommit
		}

	}
	m.mu.Unlock()

	log.Println("current", m.mapTasks, m.reducesTasks)

	for _, v := range m.mapTasks {
		if v != TaskCommit {
			return nil
		}
	}

	for _, v := range m.reducesTasks {
		if v != TaskCommit {
			return nil
		}
	}

	m.allCommitted = true
	log.Println("all tasks completed")
	return nil

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

	// Your code here.

	return m.allCommitted
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
		workerCommit: make(map[string]int),
		allCommitted: false,
		timeout:      500 * time.Millisecond,
		workerNum:    0,
	}

	// Your code here.
	log.Println("[init] with:", files, nReduce)

	m.server()
	return &m
}
