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
	maptasks map[TaskID]string   // map task is just a filename of a file to perform map onto
	mapp     map[TaskID]WorkerID // map tasks currently in progress
	mapw     map[WorkerID]TaskID // workers currently doing maps

	reducesched *Scheduler
	reducemu    sync.Mutex
	reducetasks map[TaskID]int      // reduce task is just reduce number
	reducep     map[TaskID]WorkerID // reduce tasks currently in progress
	reducew     map[WorkerID]TaskID // workers currently doing reduces
	reducefs    [][]string          // reducefs[i] stores the inputs to the i-th reduce
	reducen     int                 // total # of reduces
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
		mapw:     make(map[WorkerID]TaskID),

		reducesched: reducesched,
		reducetasks: reducetasks,
		reducep:     make(map[TaskID]WorkerID),
		reducew:     make(map[WorkerID]TaskID),
		reducefs:    make([][]string, reducen),
		reducen:     reducen,
	}

	go c.serve()

	return &c
}

func (c *Coordinator) GetTask(args GetTaskArgs, reply *GetTaskReply) error {
	c.mapmu.Lock()
	defer c.mapmu.Unlock()

	mtaskID, mstatus := c.mapsched.GetTask()

	if mstatus == SchedulerStatusTaskAvailable {
		c.mapp[mtaskID] = args.WorkerID
		c.mapw[args.WorkerID] = mtaskID

		reply.Type = TaskTypeMap
		reply.Status = TaskStatusTaskAvailable
		reply.Filenames = []string{c.maptasks[mtaskID]}
		reply.Partitions = c.reducen

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
		c.reducep[rtaskID] = args.WorkerID
		c.reducew[args.WorkerID] = rtaskID

		reply.Type = TaskTypeReduce
		reply.Status = TaskStatusTaskAvailable
		reply.Filenames = c.reducefs[c.reducetasks[rtaskID]]
		reply.Partitions = c.reducetasks[rtaskID]

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
		mtaskID, has := c.mapw[args.WorkerID]
		if !has {
			return nil
		}

		if c.mapp[mtaskID] == args.WorkerID {
			c.mapsched.TaskDone(mtaskID)
			delete(c.mapp, mtaskID)
			delete(c.maptasks, mtaskID)

			for i := 0; i < len(args.Filenames); i++ {
				c.reducefs[i] = append(c.reducefs[i], args.Filenames[i])
			}
		}
	}

	if args.Type == TaskTypeReduce {
		rtaskID, has := c.reducew[args.WorkerID]
		if !has {
			return nil
		}

		if c.reducep[rtaskID] == args.WorkerID {
			c.reducesched.TaskDone(rtaskID)
			delete(c.reducep, rtaskID)
			delete(c.reducetasks, rtaskID)
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
