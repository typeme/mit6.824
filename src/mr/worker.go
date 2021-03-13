package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

type worker struct {
	id      string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegWorker", args, reply); !ok {
		log.Fatal("reg fail")
	}
	w.id = reply.WorkId
}

func (w *worker) run() {
	retry := 3
	for {
		args := WorkArgs{WorkId: w.id}
		reply := WorkReply{}
		working := call("Master.Work", &args, &reply)

		if reply.IsFinished || !working {
			log.Println("finished")
			return
		}

		switch reply.TaskName {
		case "map":
			mapWork(reply, w.mapf)
			retry = 3

		case "reduce":
			reduceWork(reply, w.reducef)
			retry = 3
		default:
			log.Println("error reply: would retry times: ", retry)
			if retry < 0 {
				return
			}
			retry--
		}

		commitArgs := CommitArgs{WorkerId: w.id, TaskId: reply.TaskId, TaskName: reply.TaskName}
		commitReply := CommitReply{}
		_ = call("Master.Commit", &commitArgs, &commitReply)

		time.Sleep(2000 * time.Millisecond)
	}
}

func mapWork(task WorkReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	kvs := mapf(task.FileName, string(content))

	sort.Sort(ByKey(kvs))

	tempName := "mr-tmp-" + strconv.Itoa(task.TaskId)
	fileBucket := make(map[int]*json.Encoder)
	for i := 0; i < task.BucketName; i++ {
		ofile, _ := os.Create(tempName + "-" + strconv.Itoa(i))
		defer ofile.Close()
		fileBucket[i] = json.NewEncoder(ofile)
	}

	for _, kv := range kvs {
		key := kv.Key
		reduce_idx := ihash(key) % task.BucketName
		err := fileBucket[reduce_idx].Encode(&kv)
		if err != nil {
			log.Fatal("Unable to write to file")
		}

	}
}

func reduceWork(task WorkReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for mapTaskNumber := 0; mapTaskNumber < task.BucketName; mapTaskNumber++ {
		fileName := "mr-tmp-" + strconv.Itoa(mapTaskNumber) + "-" + strconv.Itoa(task.TaskId)
		f, err := os.Open(fileName)
		defer f.Close()
		if err != nil {
			log.Println(err)
			log.Fatal("Unable to read from: ", fileName)
		}

		decoder := json.NewDecoder(f)
		var kv KeyValue
		for decoder.More() {
			err := decoder.Decode(&kv)
			if err != nil {
				log.Fatal("Json decode failed, ", err)
			}
			intermediate = append(intermediate, kv)
		}

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-"
		ofile, err := os.Create(oname + strconv.Itoa(task.TaskId+1))
		defer ofile.Close()
		if err != nil {
			log.Fatal("Unable to create file: ", ofile)
		}
		log.Println("complete to ", task.TaskId, "start to write in to ", ofile)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
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

	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
