package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

// process a given map task
func HandleMapTask(mapf func(string, string) []KeyValue, filename string, nReduce int, inputFileNumber int) {
	// create the 2d array to hold intermdiate data
	intermediate := [][]KeyValue{}
	for i := 0; i < nReduce; i++ {
		intermediate = append(intermediate, []KeyValue{})
	}

	// read each input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// pass the content to Map
	kvas := mapf(filename, string(content))

	// accumulate the intermediate Map output
	for _, kva := range kvas {
		bucket := ihash(kva.Key) % nReduce
		intermediate[bucket] = append(intermediate[bucket], kva)
	}

	// write the map output to a file
	var onames []string
	var tempFiles []string
	for i := 0; i < nReduce; i++ {
		// store the output in a temporary file (in case two workers executing the same task)
		tempName := "mr-tmp-" + strconv.Itoa(inputFileNumber) + "-" + strconv.Itoa(i)
		tempFile, err := ioutil.TempFile(".", tempName)
		if err != nil {
			log.Fatalf("cannot create temp file %v", tempName)
		}
		tempName = tempFile.Name()
		defer os.Remove(tempName)
		for _, kva := range intermediate[i] {
			fmt.Fprintf(tempFile, "%v %v\n", kva.Key, kva.Value)
		}
		tempFile.Close()
		tempFiles = append(tempFiles, tempName)

		// atomically rename the file
		oname := "mr-intermediate-" + strconv.Itoa(inputFileNumber) + "-" + strconv.Itoa(i)
		err = os.Rename(tempName, oname)
		if err != nil {
			log.Fatalf("cannot rename file %s to %s", tempName, oname)
		}
		onames = append(onames, oname)
	}
	// fmt.Println("Mapper wrote file:", inputFileNumber, " to tempfiles:", tempFiles, " and files", onames)
}

// process a given reduce task
func HandleReduceTask(reducef func(string, []string) string, nMapper int, reducerID int) {
	// create the array to hold intermdiate data
	intermediate := []KeyValue{}

	// read each intermediate input file
	for i := 0; i < nMapper; i++ {
		filename := "mr-intermediate-" + strconv.Itoa(i) + "-" + strconv.Itoa(reducerID)
		// open and read file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// parse content
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			kva := strings.Split(string(line), " ")
			if len(kva) == 2 {
				intermediate = append(intermediate, KeyValue{kva[0], kva[1]})
			} else {
				break
			}
		}
	}

	// sort the file to pass it to reduce
	sort.Sort(ByKey(intermediate))

	// create a temporary file to write the output to
	tempName := "mr-out-tmp-" + strconv.Itoa(reducerID)
	tempFile, err := ioutil.TempFile(".", tempName)
	if err != nil {
		log.Fatalf("cannot create temp file %v", tempName)
	}
	tempName = tempFile.Name()
	defer os.Remove(tempName)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to tempName
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()

	// atomically rename the file
	oname := "mr-out-" + strconv.Itoa(reducerID)
	err = os.Rename(tempName, oname)
	if err != nil {
		log.Fatalf("cannot rename file %s to %s", tempName, oname)
	}
	// fmt.Println("Mapper ID:", reducerID, " wrote to temp file", tempName, " and file", oname)
}

//
// main/mrworker.go calls this function.
// calls the master preiodically asking for a task
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	pid := os.Getpid()
	// i, j := 0, 0 // uncomment to test crashing
	for true {
		task := GetTask(pid)
		if task.TaskType == 0 {
			// uncomment to test crashing
			// if task.InputFileNumber == 0 && i == 0 {
			// 	fmt.Println("Sleeping Worker pid:", pid, " MapTask:", task.TaskType, task.InputFileName, task.InputFileNumber, task.NumberOfWorkers, task.ReducerID)
			// 	time.Sleep(time.Second * 20)
			// }
			// fmt.Printf("Worker with pid: %d received MapTask: <%s, %d>\n", pid, task.InputFileName, task.InputFileNumber)
			HandleMapTask(mapf, task.InputFileName, task.NumberOfWorkers, task.InputFileNumber)
			// Tell the master we are done with mapping
			// fmt.Printf("Worker with pid: %d completed MapTask:%s\n", pid, task.InputFileName)
			ReplyMapTask(task.InputFileNumber)
		} else if task.TaskType == 1 {
			// uncomment to test crashing
			// if task.ReducerID == 0 && j == 0 {
			// 	fmt.Println("Sleeping Worker pid:", pid, " ReduceTask:", task.TaskType, task.InputFileName, task.InputFileNumber, task.NumberOfWorkers, task.ReducerID)
			// 	time.Sleep(time.Second * 20)
			// }
			// j++
			// fmt.Printf("Worker with pid: %d received ReduceTask:%d\n", pid, task.ReducerID)
			HandleReduceTask(reducef, task.NumberOfWorkers, task.ReducerID)
			// Tell the master we are done with reducing
			// fmt.Printf("Worker with pid: %d completed ReduceTask:%d\n", pid, task.ReducerID)
			ReplyReduceTask(task.ReducerID)
		} else if task.TaskType == 2 {
			// fmt.Println("Worker pid:", pid, "received WaitTask", "waiting for a second")
			time.Sleep(time.Second)
		} else {
			// fmt.Println("Worker pid:", pid, "received QuitTask")
			break
		}
		// i++
	}
}

func GetTask(pid int) MasterGetTaskReplyArgs {
	args := MasterGetTaskCallArgs{}
	args.PID = pid
	reply := MasterGetTaskReplyArgs{}
	// send the RPC request, wait for the reply.
	call("Master.GetTask", &args, &reply)
	return reply
}

func ReplyMapTask(fileNumber int) MasterDoneMapTaskReplyArgs {
	args := MasterDoneMapTaskCallArgs{}
	args.FileNumber = fileNumber
	reply := MasterDoneMapTaskReplyArgs{}
	// send the RPC request, wait for the reply.
	call("Master.ReplyMapTask", &args, &reply)
	return reply
}

func ReplyReduceTask(reducerID int) MasterDoneReduceTaskReplyArgs {
	args := MasterDoneReduceTaskCallArgs{}
	args.ReducerID = reducerID
	reply := MasterDoneReduceTaskReplyArgs{}
	// send the RPC request, wait for the reply.
	call("Master.ReplyReduceTask", &args, &reply)
	return reply
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
