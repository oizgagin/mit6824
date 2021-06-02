package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

const retryTimeout = time.Second

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := WorkerID("worker-" + strconv.Itoa(os.Getpid()))

LOOP:
	for {
		task, err := getTask(workerID)
		if err != nil {
			log.Printf("getTask(%v) error: %v", workerID, err)
			time.Sleep(retryTimeout)
			continue LOOP
		}

		if task.Status == TaskStatusNoTasksAvailable {
			time.Sleep(retryTimeout)
			continue LOOP
		}

		if task.Status == TaskStatusNoMoreTasks {
			return
		}

		var filenames []string

		if task.Type == TaskTypeMap {
			filenames, err = doMap(mapf, workerID, task.Filenames[0], task.Partitions)
			if err != nil {
				log.Printf("doMap(%v, %v, %v) error: %v", workerID, task.Filenames[0], task.Partitions, err)
			}
		}

		if task.Type == TaskTypeReduce {
			filename, err := doReduce(reducef, task.Filenames, task.Partitions)
			if err != nil {
				log.Printf("doReduce(%v, %v) error: %v", task.Filenames, task.Partitions, err)
			}
			filenames = []string{filename}
		}

		if err != nil {
			cleanup(filenames)
			time.Sleep(retryTimeout)
			continue LOOP
		}

		if err := markDone(workerID, task.Type, filenames); err != nil {
			log.Printf("markDone(%v, %v, %v) error: %v", workerID, task.Type, filenames, err)
			time.Sleep(retryTimeout)
		}
	}
}

func getTask(workerID WorkerID) (GetTaskReply, error) {
	args, reply := GetTaskArgs{WorkerID: workerID}, GetTaskReply{}
	err := call("Coordinator.GetTask", args, &reply)
	return reply, err
}

func markDone(workerID WorkerID, taskType TaskType, filenames []string) error {
	args := TaskDoneArgs{WorkerID: workerID, Type: taskType, Filenames: filenames}
	return call("Coordinator.TaskDone", args, &TaskDoneReply{})
}

func doMap(mapf func(string, string) []KeyValue, workerID WorkerID, filename string, partitions int) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("could not read %v: %v", filename, err)
	}

	kvs := mapf(filename, string(content))

	parts := make([][]KeyValue, partitions)
	for _, kv := range kvs {
		partno := ihash(kv.Key) % partitions
		parts[partno] = append(parts[partno], kv)
	}

	outputs := make([]string, partitions)
	for i := 0; i < partitions; i++ {
		f, err := ioutil.TempFile("", fmt.Sprintf("map-*-%v-%v.tmp", workerID, i))
		if err != nil {
			return nil, fmt.Errorf("could not create temp file: %v", err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		if err := enc.Encode(parts[i]); err != nil {
			return nil, fmt.Errorf("could not write json to %v: %v", f.Name(), err)
		}

		newname := strings.TrimRight(f.Name(), ".tmp")
		if err := os.Rename(f.Name(), newname); err != nil {
			return nil, fmt.Errorf("could not rename %v to %v: %v", f.Name(), newname, err)
		}

		outputs[i] = newname
	}

	return outputs, nil
}

func doReduce(reducef func(string, []string) string, filenames []string, partition int) (string, error) {
	var kvs []KeyValue

	for _, filename := range filenames {
		f, err := os.Open(filename)
		if err != nil {
			return "", fmt.Errorf("could not open file: %v", err)
		}
		defer f.Close()

		dec := json.NewDecoder(f)

		var kvss []KeyValue
		if err := dec.Decode(&kvss); err != nil {
			return "", fmt.Errorf("could not decode %v: %v", filename, err)
		}
		kvs = append(kvs, kvss...)
	}

	m := make(map[string][]string)
	for _, kv := range kvs {
		m[kv.Key] = append(m[kv.Key], kv.Value)
	}

	results := make([]string, 0, len(m))
	for k, vs := range m {
		results = append(results, k+" "+reducef(k, vs))
	}

	f, err := ioutil.TempFile("", fmt.Sprintf("mr-out-%v.*", partition))
	if err != nil {
		return "", fmt.Errorf("could not create temp file: %v", err)
	}
	defer f.Close()

	for _, result := range results {
		f.Write([]byte(result + "\n"))
	}
	f.Sync()

	outf := fmt.Sprintf("mr-out-%v", partition)
	if err := os.Rename(f.Name(), outf); err != nil {
		return "", fmt.Errorf("could not move %v to %v: %v", f.Name(), outf, err)
	}

	return outf, nil
}

func cleanup(filenames []string) {
	for _, filename := range filenames {
		os.Remove(filename)
	}
}

func call(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
