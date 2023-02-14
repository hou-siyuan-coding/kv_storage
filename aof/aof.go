package aof

import (
	"fmt"
	"kv_storage/datastore"
	"kv_storage/entity"
	"kv_storage/executer"
	"kv_storage/parser"
	"os"
	"strconv"
	"time"

	"github.com/hdt3213/godis/lib/logger"
)

type AofInstance struct {
	file  *os.File
	cmdCh chan parser.Payload
}

func NewAofInstance(fileName string) *AofInstance {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err.Error())
	}
	return &AofInstance{file: file, cmdCh: make(chan parser.Payload, 16)}
}

func (a *AofInstance) ToCmdCh(payload *parser.Payload) {
	// fmt.Println("cmd to buffer")
	a.cmdCh <- *payload
}

func (a *AofInstance) Persist() {
	fmt.Println("start aof instance to persist cmd")
	for payload := range a.cmdCh {
		cmdBytes := payload.Data.ToBytes()
		a.file.Write(cmdBytes)
	}
}

func (a *AofInstance) Close() {
	a.file.Close()
}

func (a *AofInstance) Init(db *datastore.Map, execInstance *executer.Executer) error {
	fmt.Println("building data initial state according to " + a.file.Name())
	ch := make(chan *parser.Payload)
	go parser.Parse0(a.file, ch)
	timer := time.NewTimer(time.Second)
	flag := true
	for flag {
		select {
		case payload := <-ch:
			if payload.Err != nil {
				return payload.Err
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
			execInstance.Execute(r.Args)
		case <-timer.C:
			flag = false
		}
	}
	fmt.Println("build completed")
	return nil
}

func ExpireToExpireAt(args [][]byte) [][]byte {
	var cmd [][]byte
	if len(args) < 3 {
		return args
	}
	sec, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return args
	}
	return append(cmd, []byte("expireat"), args[1], []byte(fmt.Sprint(time.Now().Unix()+int64(sec))))
}
