package main

import (
	"sync"
	"time"

	"github.com/oizgagin/mit6824/mapreduce/rpc"
)

type Coordinator struct {
	mapsched *Scheduler
	mapmu    sync.Mutex
	maptasks map[TaskID]string       // map task is just a filename of a file to perform map onto
	mapp     map[TaskID]rpc.WorkerID // map tasks currently in progress
	mapw     map[rpc.WorkerID]TaskID // workers currently doing maps

	reducesched *Scheduler
	reducemu    sync.Mutex
	reducetasks map[TaskID]int          // reduce task is just reduce number
	reducep     map[TaskID]rpc.WorkerID // reduce tasks currently in progress
	reducew     map[rpc.WorkerID]TaskID // workers currently doing reduces
	reducefs    [][]string              // reducefs[i] stores the inputs to the i-th reduce
	reducen     int                     // total # of reduces
}

func MakeCoordinator(files []string, mapTimeout, reduceTimeout time.Duration, reducen int) *Coordinator {
	mapsched := NewScheduler(mapTimeout)
	maptasks := make(map[TaskID]string)
	for _, file := range files {
		maptasks[mapsched.AddTask()] = file
	}

	reducesched := NewScheduler(reduceTimeout)
	reducetasks := make(map[TaskID]int)
	for i := 0; i < reducen; i++ {
		reducetasks[reducesched.AddTask()] = i
	}

	return &Coordinator{
		mapsched: mapsched,
		maptasks: maptasks,
		mapp:     make(map[TaskID]rpc.WorkerID),
		mapw:     make(map[rpc.WorkerID]TaskID),

		reducesched: reducesched,
		reducetasks: reducetasks,
		reducep:     make(map[TaskID]rpc.WorkerID),
		reducew:     make(map[rpc.WorkerID]TaskID),
		reducefs:    make([][]string, reducen),
		reducen:     reducen,
	}
}

func (c *Coordinator) GetTask(args rpc.GetTaskArgs, reply *rpc.GetTaskReply) error {
	c.mapmu.Lock()
	defer c.mapmu.Unlock()

	mtaskID, mstatus := c.mapsched.GetTask()

	if mstatus == SchedulerStatusTaskAvailable {
		c.mapp[mtaskID] = args.WorkerID
		c.mapw[args.WorkerID] = mtaskID

		reply.Type = rpc.TaskTypeMap
		reply.Status = rpc.TaskStatusTaskAvailable
		reply.Filenames = []string{c.maptasks[mtaskID]}
		reply.Partitions = c.reducen

		return nil
	}

	if mstatus == SchedulerStatusNoTasksAvailable {
		reply.Status = rpc.TaskStatusNoTasksAvailable
		return nil
	}

	c.reducemu.Lock()
	defer c.reducemu.Unlock()

	rtaskID, rstatus := c.reducesched.GetTask()

	if rstatus == SchedulerStatusTaskAvailable {
		c.reducep[rtaskID] = args.WorkerID
		c.reducew[args.WorkerID] = rtaskID

		reply.Type = rpc.TaskTypeReduce
		reply.Status = rpc.TaskStatusTaskAvailable
		reply.Filenames = c.reducefs[c.reducetasks[rtaskID]]
		reply.Partitions = c.reducetasks[rtaskID]

		return nil
	}

	if rstatus == SchedulerStatusNoTasksAvailable {
		reply.Status = rpc.TaskStatusNoTasksAvailable
		return nil
	}

	reply.Status = rpc.TaskStatusNoMoreTasks

	return nil
}

func (c *Coordinator) TaskDone(args rpc.TaskDoneArgs, reply *rpc.TaskDoneReply) error {
	c.mapmu.Lock()
	defer c.mapmu.Unlock()

	c.reducemu.Lock()
	defer c.reducemu.Unlock()

	if args.Type == rpc.TaskTypeMap {
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

	if args.Type == rpc.TaskTypeReduce {
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
