package main

import (
	"sync"
	"time"
)

type TaskID int

const (
	EmptyTaskID TaskID = -1
)

type Scheduler struct {
	mu sync.Mutex

	total      int                      // # of tasks to be done
	done       int                      // # of finished tasks
	queue      []TaskID                 // tasks queue
	inprogress map[TaskID]chan struct{} // task to its done channel

	timeout time.Duration

	nextid int
}

func NewScheduler(timeout time.Duration) *Scheduler {
	return &Scheduler{
		inprogress: make(map[TaskID]chan struct{}),
		timeout:    timeout,
		nextid:     1,
	}
}

func (s *Scheduler) AddTask() TaskID {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := TaskID(s.nextid)
	s.nextid++

	s.total++
	s.queue = append(s.queue, id)

	return id
}

func (s *Scheduler) Done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.done == s.total
}

func (s *Scheduler) TaskDone(id TaskID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, in := s.inprogress[id]; in {
		s.done++
		close(s.inprogress[id])
		delete(s.inprogress, id)
	}
}

type SchedulerStatus int

const (
	SchedulerStatusNoTasksAvailable SchedulerStatus = iota + 1
	SchedulerStatusTaskAvailable
	SchedulerStatusDone
)

func (s *Scheduler) GetTask() (TaskID, SchedulerStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done == s.total {
		return EmptyTaskID, SchedulerStatusDone
	}

	if len(s.queue) == 0 {
		return EmptyTaskID, SchedulerStatusNoTasksAvailable
	}

	id := s.queue[len(s.queue)-1]
	s.queue = s.queue[:len(s.queue)-1]

	done := make(chan struct{})
	s.inprogress[id] = done

	go s.watch(id, done)

	return id, SchedulerStatusTaskAvailable
}

func (s *Scheduler) watch(id TaskID, done chan struct{}) {
	select {
	case <-done:
		return

	case <-time.After(s.timeout):
		s.mu.Lock()
		defer s.mu.Unlock()

		// TaskDone() could be called while we were waiting for lock, so double check
		select {
		case <-done:
			return
		default:
		}

		s.queue = append(s.queue, id)
		delete(s.inprogress, id)
	}
}
