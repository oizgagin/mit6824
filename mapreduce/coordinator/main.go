package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	mrrpc "github.com/oizgagin/mit6824/mapreduce/rpc"
)

var (
	mapTimeout    = flag.Duration("map-timeout", 10*time.Second, "timeout for map tasks")
	reduceTimeout = flag.Duration("reduce-timeout", 10*time.Second, "timeout for reduce tasks")
	reduceNo      = flag.Int("reduce-no", 10, "number of reduces")
)

func init() {
	flag.Parse()
}

func main() {
	c := MakeCoordinator(flag.Args(), *mapTimeout, *reduceTimeout, *reduceNo)
	go serve(c)

	for c.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}

func serve(c *Coordinator) {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := mrrpc.CoordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
