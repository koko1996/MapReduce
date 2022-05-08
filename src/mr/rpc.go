package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// RPC definitions

// Struct for GetTask Call
type MasterGetTaskCallArgs struct {
	PID int // Calling process's ID
}

// Struct for GetTask Reply
type MasterGetTaskReplyArgs struct {
	TaskType        int    // 0 for map, 1 for reduce, 2 for wait, anything else for done
	InputFileName   string // only for map task as it tells the worker which file to process
	InputFileNumber int    // only for map task as it tells the worker which mapper it is
	NumberOfWorkers int    // tells the number of reducers for Map tasks and tells the number of Mappers for reduce tasks
	ReducerID       int    // only for reduce tasks as it tells the worker which reducer it is
}

// Struct for ReplyMapTask Call
type MasterDoneMapTaskCallArgs struct {
	FileNumber int // the number of the file that the mapper finished processing
}

// Struct for ReplyMapTask reply (not used)
type MasterDoneMapTaskReplyArgs struct {
}

// Struct for ReplyReduceTask Call
type MasterDoneReduceTaskCallArgs struct {
	ReducerID int // the ID of the reduce task that the worker finished processing
}

// Struct for ReplyReduceTask reply (not used)
type MasterDoneReduceTaskReplyArgs struct {
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
