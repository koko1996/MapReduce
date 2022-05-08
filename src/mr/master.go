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

type MapState struct {
	files          []string    // input files to be processed
	filesStatus    []int       // 0 haven't started, 1 started, 2 finished
	filesStartTime []time.Time // lastest time at which a worker started processing the correspond file in mapFiles
	startCount     int         // number of files that at least one worker started processing
	doneCount      int         // number of files that at least one worker finished processing
	mutex          sync.Mutex  // Mutex for objects related to map
}

type ReduceState struct {
	count         int         // number of reducer jobs
	jobsStatus    []int       // 0 haven't started, 1 started, 2 finished
	jobsStartTime []time.Time // lastest time at which a worker started processing the correspond reducer job
	startCount    int         // number of files that at least one worker started processing
	doneCount     int         // number of files that at least one worker finished processing
	mutex         sync.Mutex  // Mutex for objects related to reduce
}

type Master struct {
	mapS    MapState
	reduceS ReduceState
}

/* global variable*/
var m Master

// returns the next map task if there is any
func (m *Master) getNextMapTask() (bool, string, int) {
	m.mapS.mutex.Lock()
	defer m.mapS.mutex.Unlock()
	if m.mapS.startCount < len(m.mapS.files) {
		// there is a file that we have not started mapping (that file should be mapped next)
		fileNumber := m.mapS.startCount
		m.mapS.startCount++                            // increment the number of files we have started mapping
		m.mapS.filesStatus[fileNumber] = 1             // set the status of that file to started
		m.mapS.filesStartTime[fileNumber] = time.Now() // set the time of start of that file to now
		return true, m.mapS.files[fileNumber], fileNumber
	} else {
		// we have started mapping of all files
		// look for a file that we have started mapping but it is taking long time
		for i := 0; i < len(m.mapS.files); i++ {
			if m.mapS.filesStatus[i] == 1 && time.Now().After(m.mapS.filesStartTime[i].Add(10*time.Second)) {
				// found a file that we sent to a worker more than 10 seconds ago
				// no need to change m.mapS.filesStatus[fileNumber] and m.mapS.startCount
				m.mapS.filesStartTime[i] = time.Now() // update the start time of that file
				return true, m.mapS.files[i], i
			}
		}
		return false, "", 0
	}
}

// returns the next reduce task if there is any
func (m *Master) getNextReduceTask() (bool, int) {
	m.reduceS.mutex.Lock()
	defer m.reduceS.mutex.Unlock()
	if m.reduceS.startCount < m.reduceS.count {
		// there is a reduce job that we have not started (that reduce job should be used next)
		reducerNumber := m.reduceS.startCount
		m.reduceS.startCount++                              // increment the number of reduce jobs that we have started
		m.reduceS.jobsStatus[reducerNumber] = 1             // set the status of that reduce job to started
		m.reduceS.jobsStartTime[reducerNumber] = time.Now() // set the time of start of that reduce job to now
		return true, reducerNumber
	} else {
		// we have started all reduce jobs
		// look for an unfinished reduce job that we have started a long time ago
		for i := 0; i < m.reduceS.count; i++ {
			if m.reduceS.jobsStatus[i] == 1 && time.Now().After(m.reduceS.jobsStartTime[i].Add(10*time.Second)) {
				// found a reduce job that we sent to a worker more than 10 seconds ago
				// no need to change m.reduceS.jobsStatus[fileNumber] and m.reduceS.startCount
				m.reduceS.jobsStartTime[i] = time.Now()
				return true, i
			}
		}
		return false, 0
	}
}

/* RPC handlers for the worker to call */

// worker calls this method to get a task
func (m *Master) GetTask(args *MasterGetTaskCallArgs, reply *MasterGetTaskReplyArgs) error {
	// read safely
	m.mapS.mutex.Lock()
	mapDoneCount := m.mapS.doneCount
	m.mapS.mutex.Unlock()
	if mapDoneCount < len(m.mapS.files) {
		// try to send a map task
		taskExists, fileName, fileNumber := m.getNextMapTask()
		if taskExists {
			// send a map task
			reply.TaskType = 0
			reply.InputFileName, reply.InputFileNumber = fileName, fileNumber
			reply.NumberOfWorkers = m.reduceS.count
			return nil
		} else {
			// tell the worker to wait for a bit
			reply.TaskType = 2
			return nil
		}
	}

	// read safely
	m.reduceS.mutex.Lock()
	reduceDoneCount := m.reduceS.doneCount
	m.reduceS.mutex.Unlock()
	if reduceDoneCount < m.reduceS.count {
		// try to send a reduce task
		taskExists, reducerNumber := m.getNextReduceTask()
		if taskExists {
			// send a reduce task
			reply.TaskType = 1
			reply.NumberOfWorkers = len(m.mapS.files)
			reply.ReducerID = reducerNumber
			return nil
		} else {
			// tell the worker to wait for a bit
			reply.TaskType = 2
			return nil
		}
	}

	// no more tasks left
	// tell the worker to quit by sending a quit task
	reply.TaskType = 3
	return nil

}

// worker calls this method when it finishes a map task
func (m *Master) ReplyMapTask(args *MasterDoneMapTaskCallArgs, reply *MasterDoneMapTaskReplyArgs) error {
	m.mapS.mutex.Lock()
	m.mapS.filesStatus[args.FileNumber] = 2 // set the status of the map task to complete
	m.mapS.doneCount++                      // increment the number of completed map tasks
	m.mapS.mutex.Unlock()
	return nil
}

// worker calls this method when it finishes a reduce task
func (m *Master) ReplyReduceTask(args *MasterDoneReduceTaskCallArgs, reply *MasterDoneReduceTaskReplyArgs) error {
	m.reduceS.mutex.Lock()
	m.reduceS.jobsStatus[args.ReducerID] = 2 // set the status of the reduce task to complete
	m.reduceS.doneCount++                    // increment the number of completed reduce tasks
	m.reduceS.mutex.Unlock()
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
// returns true if all the reduce jobs have finished
//
func (m *Master) Done() bool {
	m.reduceS.mutex.Lock()
	defer m.reduceS.mutex.Unlock()
	// fmt.Println("MASTER: has finished", m.reduceS.doneCount, "out of", m.reduceS.count, "reduce jobs")
	return (m.reduceS.doneCount == m.reduceS.count)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m = Master{}
	// initialize the values in m
	m.mapS.files = files
	m.mapS.filesStatus = make([]int, len(files))
	m.mapS.filesStartTime = make([]time.Time, len(files))
	m.mapS.startCount = 0
	m.mapS.doneCount = 0

	m.reduceS.count = nReduce
	m.reduceS.jobsStatus = make([]int, nReduce)
	m.reduceS.jobsStartTime = make([]time.Time, nReduce)
	m.reduceS.startCount = 0
	m.reduceS.doneCount = 0

	// fmt.Printf("MASTER: recieved %d files and will use %d reducers\n", len(files), nReduce)

	m.server()
	return &m
}
