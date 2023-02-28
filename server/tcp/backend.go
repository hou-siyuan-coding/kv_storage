package tcp

import (
	"fmt"
	"io"
	"kv_storage/algorithm"
	"kv_storage/aof"
	"kv_storage/config"
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
	"time"
)

type Backend struct {
	connectionNum uint16
	ConnWg        *sync.WaitGroup
	executer      *executer.Executer
	aof           *aof.AofInstance
	address       string
	listener      net.Listener

	isCluster    bool
	peers        []string
	deadPeers    map[string]struct{}
	innerConns   map[string]net.Conn
	osSignalChan chan os.Signal
}

func NewBackend(config *config.Config) *Backend {
	db := datastore.NewMap()
	aofInstance := aof.NewAofInstance(config.AofFile)
	execInstance := executer.NewExecuter(db)
	aofInstance.Init(db, execInstance)
	backend := &Backend{
		ConnWg:       &sync.WaitGroup{},
		executer:     execInstance,
		aof:          aofInstance,
		address:      fmt.Sprint(config.Bind, ":", config.Port),
		isCluster:    config.IsCluster,
		deadPeers:    make(map[string]struct{}),
		innerConns:   make(map[string]net.Conn),
		osSignalChan: make(chan os.Signal, 1),
	}
	if config.IsCluster {
		backend.peers = config.Peers
		algorithm.Consistenthash.AddNode(backend.address)
		fmt.Printf("%v添加%v到哈希环上\n", backend.address, backend.address)
		for _, address := range backend.peers {
			algorithm.Consistenthash.AddNode(address)
			fmt.Printf("%v添加%v到哈希环上\n", backend.address, address)
		}
		fmt.Printf("%v节点的哈希环%v\n", backend.address, algorithm.Consistenthash.GetKeys())
	}
	return backend
}

func (backend *Backend) Handle(conn net.Conn) {
	defer func() {
		conn.Close()
		fmt.Printf("server instance %v connection is completed and closed!\n", backend.address)
	}()
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
				fmt.Println("connection closed")
				return
			}
			continue
		}
		if payload.Data == nil {
			fmt.Println("empty payload")
			continue
		}
		r, ok := payload.Data.(*entity.MultiBulkReply)
		if !ok {
			fmt.Println("require multi bulk protocol")
			continue
		}
		if len(r.Args) > 1 && string(r.Args[0]) == "expire" {
			r.Args = aof.ExpireToExpireAt(r.Args)
		}
		if ok, reply := backend.resend(r.Args); ok {
			conn.Write(reply.ToBytes())
			continue
		}
		reply := backend.executer.Execute(r.Args)
		go backend.aof.ToCmdCh(payload)
		conn.Write(reply.ToBytes())
	}
}

func (backend *Backend) Start() {
	if backend.isCluster {
		go backend.Heartbeat()
	}
	backend.listener, _ = net.Listen("tcp", backend.address)
	fmt.Printf("server is listening in %v!\n", backend.address)
	signal.Notify(backend.osSignalChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-backend.osSignalChan
		close(backend.osSignalChan)
		backend.Stop()
	}()

	go backend.aof.Persist()

	for {
		fmt.Printf("%v waiting for connection\n", backend.address)
		conn, err := backend.listener.Accept()
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
	fmt.Println(backend.address, "hand end")
}

func (backend *Backend) Stop() {
	fmt.Printf("server %v is shutting down!\n", backend.address)
	backend.listener.Close()
	fmt.Printf("server %v listener is closed!\n", backend.address)
	if backend.isCluster {
		for _, peerConn := range backend.innerConns {
			peerConn.Close()
		}
	}
}

func (backend *Backend) resend(args [][]byte) (bool, entity.Reply) {
	if !backend.isCluster {
		return false, nil
	}
	key, isHeartbeat := executer.GetKey(args)
	if isHeartbeat || key == "" {
		return false, nil
	}
	fmt.Println("key:", key)
	nodeId := algorithm.Consistenthash.PickNode(key)
	fmt.Println("need node id:", nodeId, "current node id:", backend.address)
	if backend.address == nodeId {
		fmt.Println("dont't resend")
		return false, nil
	}
	// 转发
	re := entity.MakeMultiBulkReply(args)
	cmd := re.ToBytes()
	fmt.Println(string(cmd))
	peerConn, ok := backend.innerConns[nodeId]
	var err error
	if !ok {
		peerConn, err = net.Dial("tcp", nodeId)
		if err != nil {
			panic(err)
		}
		backend.innerConns[nodeId] = peerConn
	}
	n, err := peerConn.Write(cmd)
	if err != nil {
		panic(err)
	}
	fmt.Println("resend", n, "bytes")
	reply, timeout := parser.WaitReplyWithTime(peerConn)
	if timeout {
		return true, entity.MakeBulkReply([]byte("timeout"))
	}
	fmt.Println("resend reply:", string(reply.ToBytes()))
	return true, reply
}

func (backend *Backend) doHeartbeat() {
	for _, id := range backend.peers {
		peerConn, ok := backend.innerConns[id]
		var err error
		if !ok {
			peerConn, err = net.Dial("tcp", id)
			if err == nil {
				delete(backend.deadPeers, id)
				backend.innerConns[id] = peerConn
				fmt.Printf("%v reactive node %v\n", backend.address, id)
			} else {
				continue
			}
		}
		_, err = peerConn.Write(entity.MakeMultiBulkReply([][]byte{[]byte("ping")}).ToBytes())
		if err != nil {
			backend.deadPeers[id] = struct{}{}
			delete(backend.innerConns, id)
			fmt.Printf("%v dead node %v\n", backend.address, id)
		} else if _, timeout := parser.WaitReplyWithTime(peerConn); timeout {
			backend.deadPeers[id] = struct{}{}
			delete(backend.innerConns, id)
			fmt.Printf("%v dead node %v\n", backend.address, id)
		}
	}
	fmt.Printf("%v's peers: ", backend.address)
	for _, id := range backend.peers {
		if _, ok := backend.innerConns[id]; ok {
			fmt.Printf("%v ", id)
		}
	}
	fmt.Println()
}

func (backend *Backend) Heartbeat() {
	life := time.Duration(3) * time.Second
	timer := time.NewTimer(life)
	for {
		select {
		case <-timer.C:
			backend.doHeartbeat()
			timer.Reset(life)
		case <-backend.osSignalChan:
			fmt.Printf("%v stop heartbeat!", backend.address)
			return
		}
	}
}
