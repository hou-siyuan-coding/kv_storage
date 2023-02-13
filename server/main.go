package main

import (
	"kv_storage/server/tcp"
)

func main() {
	backend := tcp.NewBackend("../backups/cmdLog.txt")
	backend.ListenAndServeWithSignal("localhost:8001")
}
