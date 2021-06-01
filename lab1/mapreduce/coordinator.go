package mapreduce

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mapsched *Scheduler
	mapmu    sync.Mutex
	maptasks map[TaskID]string
	mapp     map[TaskID]WorkerID

	reducesched *Scheduler
	reducemu    sync.Mutex
	reducetasks map[TaskID]int
	reducep     map[TaskID]WorkerID
	reducefs    [][]string
	reducen     int
}

const DefaultTaskTimeout = 10 * time.Second

func MakeCoordinator(files []string, reducen int) *Coordinator {
	mapsched := NewScheduler(DefaultTaskTimeout)
	maptasks := make(map[TaskID]string)
	for _, file := range files {
		maptasks[mapsched.AddTask()] = file
	}

	reducesched := NewScheduler(DefaultTaskTimeout)
	reducetasks := make(map[TaskID]int)
	for i := 0; i < reducen; i++ {
		reducetasks[reducesched.AddTask()] = i
	}

	c := Coordinator{
		mapsched: mapsched,
		maptasks: maptasks,
		mapp:     make(map[TaskID]WorkerID),

		reducesched: reducesched,
		reducetasks: reducetasks,
		reducep:     make(map[TaskID]WorkerID),
		reducefs:    make([][]string, reducen),
		reducen:     reducen,
	}

	go c.serve()

	return &c
}

func (c *Coordinator) GetTask(args GetTaskArgs, reply *GetTaskReply) error {
	workerID := args.WorkerID

	c.mapmu.Lock()
	defer c.mapmu.Unlock()

	mtaskID, mstatus := c.mapsched.GetTask()

	if mstatus == SchedulerStatusTaskAvailable {
		c.mapp[mtaskID] = workerID
		reply.Type = TaskTypeMap
		reply.Status = TaskStatusTaskAvailable
		reply.Filenames = []string{c.maptasks[mtaskID]}
		reply.Partitions = c.reducen
		reply.TaskID = mtaskID
		return nil
	}
	if mstatus == SchedulerStatusNoTasksAvailable {
		reply.Status = TaskStatusNoTasksAvailable
		return nil
	}

	c.reducemu.Lock()
	defer c.reducemu.Unlock()

	rtaskID, rstatus := c.reducesched.GetTask()

	if rstatus == SchedulerStatusTaskAvailable {
		c.reducep[rtaskID] = workerID
		reply.Type = TaskTypeReduce
		reply.Status = TaskStatusTaskAvailable
		reply.Filenames = c.reducefs[c.reducetasks[rtaskID]]
		reply.Partitions = c.reducetasks[rtaskID]
		reply.TaskID = rtaskID
		return nil
	}
	if rstatus == SchedulerStatusNoTasksAvailable {
		reply.Status = TaskStatusNoTasksAvailable
		return nil
	}

	reply.Status = TaskStatusNoMoreTasks
	return nil
}

func (c *Coordinator) TaskDone(args TaskDoneArgs, reply *TaskDoneReply) error {
	c.mapmu.Lock()
	defer c.mapmu.Unlock()

	c.reducemu.Lock()
	defer c.reducemu.Unlock()

	if args.Type == TaskTypeMap {
		if c.mapp[args.TaskID] == args.WorkerID {
			c.mapsched.TaskDone(args.TaskID)
			delete(c.mapp, args.TaskID)
			delete(c.maptasks, args.TaskID)

			for i := 0; i < len(args.Filenames); i++ {
				c.reducefs[i] = append(c.reducefs[i], args.Filenames[i])
			}
		}
	}

	if args.Type == TaskTypeReduce {
		if c.reducep[args.TaskID] == args.WorkerID {
			c.reducesched.TaskDone(args.TaskID)
			delete(c.reducep, args.TaskID)
			delete(c.reducetasks, args.TaskID)
		}
	}

	return nil
}

func (c *Coordinator) Done() bool {
	return c.reducesched.Done()
}

func (c *Coordinator) serve() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
