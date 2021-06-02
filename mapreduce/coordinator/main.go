package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	mrrpc "github.com/oizgagin/mit6824/mapreduce/rpc"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: coordinator inputfiles...\n")
		os.Exit(1)
	}

	c := MakeCoordinator(os.Args[1:], 10)
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
