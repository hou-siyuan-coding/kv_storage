package main

import (
	"kv_storage/server/tcp"
	"sync"
)

func main() {
	backend := tcp.Backend{ConnWg: &sync.WaitGroup{}}
	backend.ListenAndServeWithSignal("localhost:8001")
}
