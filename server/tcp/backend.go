package tcp

import (
	"fmt"
	"io"
	"kv_storage/aof"
	"kv_storage/datastore"
	"kv_storage/entity"
	"kv_storage/executer"
	"kv_storage/parser"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/hdt3213/godis/lib/logger"
)

type Backend struct {
	connectionNum uint16
	ConnWg        *sync.WaitGroup
	executer      *executer.Executer
	aof           *aof.AofInstance
}

func NewBackend(aofFile string) *Backend {
	db := datastore.NewMap()
	aofInstance := aof.NewAofInstance(aofFile)
	execInstance := executer.NewExecuter(db)
	aofInstance.Init(db, execInstance)
	return &Backend{
		ConnWg:   &sync.WaitGroup{},
		executer: execInstance,
		aof:      aofInstance,
	}
}

func (backend *Backend) Handle(conn net.Conn) {
	defer conn.Close()
	backend.connectionNum++
	fmt.Println("handing connection ..., connection number:", backend.connectionNum)

	ch := parser.ParseStream(conn)
	for payload := range ch {
		// fmt.Println(payload.Data.ToBytes(), "\r\n" + string(payload.Data.ToBytes()))
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				// h.closeClient(client)
				// logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			errReply := entity.MakeErrReply(payload.Err.Error())
			_, err := conn.Write(errReply.ToBytes())
			if err != nil {
				// h.closeClient(client)
				// logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*entity.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		if len(r.Args) > 1 && string(r.Args[0]) == "expire" {
			r.Args = aof.ExpireToExpireAt(r.Args)
		}
		reply := backend.executer.Execute(r.Args)
		go backend.aof.ToCmdCh(payload)
		conn.Write(reply.ToBytes())

	}
	fmt.Println("handle end, close conn")
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

	go backend.aof.Persist()

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
