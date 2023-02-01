package tcp

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Backend struct {
	connectionNum uint16
	ConnWg        *sync.WaitGroup
}

func (backend *Backend) Handle(conn net.Conn) {
	defer conn.Close()
	backend.connectionNum++
	fmt.Println("handing connection ..., connection number:", backend.connectionNum)
}

func (backend *Backend) ListenAndServeWithSignal(addr string) {
	listener, _ := net.Listen("tcp", addr)
	fmt.Println("backend server is running!")

	osSignalChan := make(chan os.Signal, 1)
	signal.Notify(osSignalChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-osSignalChan
		fmt.Println("received the close signal!")
		listener.Close()
	}()

	for {
		fmt.Println("wait for connection")
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("listener accept error:", err.Error())
			break
		}
		backend.ConnWg.Add(1)
		go func() {
			defer backend.ConnWg.Done()
			backend.Handle(conn)
		}()
	}
	backend.ConnWg.Wait()
}
