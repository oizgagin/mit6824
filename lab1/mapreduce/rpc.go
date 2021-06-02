package mapreduce

import (
	"os"
	"strconv"
)

type WorkerID string

type GetTaskArgs struct {
	WorkerID WorkerID
}

type TaskType string

const (
	TaskTypeMap    TaskType = "TASK_TYPE_MAP"
	TaskTypeReduce TaskType = "TASK_TYPE_REDUCE"
)

type TaskStatus string

const (
	TaskStatusTaskAvailable    TaskStatus = "TASK_STATUS_TASK_AVAILABLE"
	TaskStatusNoTasksAvailable TaskStatus = "TASK_STATUS_NO_TASKS_AVAILABLE"
	TaskStatusNoMoreTasks      TaskStatus = "TASK_STATUS_NO_MORE_TASKS"
)

type GetTaskReply struct {
	Type       TaskType
	Status     TaskStatus
	Filenames  []string
	Partitions int
}

type TaskDoneArgs struct {
	WorkerID  WorkerID
	Type      TaskType
	Filenames []string
}

type TaskDoneReply struct{}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
