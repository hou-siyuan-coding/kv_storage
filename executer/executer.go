package executer

import (
	"kv_storage/datastore"
	"kv_storage/entity"
	"strconv"
	"time"
)

const (
	ParamNotFoundErr       = "parameter can not found"
	MissParamErr           = "miss parameter"
	ParamNotImplementedErr = "current commonds can not implemented"
	NoErr                  = "+OK"
)

type Executer struct {
	db *datastore.Map
}

func NewExecuter(store *datastore.Map) *Executer {
	return &Executer{db: store}
}

func (e *Executer) Execute(args [][]byte) entity.Reply {
	if len(args) == 0 {
		return entity.MakeErrReply(ParamNotFoundErr)
	}
	switch string(args[0]) {
	case "set":
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		e.db.Set(args[1], args[2])
		return entity.MakeStatusReply(NoErr)
	case "get":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		value, _, err := e.db.Get(args[1])
		if err == datastore.ErrKeyExpired {
			e.db.Del(args[1])
		}
		return entity.MakeBulkReply(value)
	case "mset":
		for i := 1; i < len(args)-1; {
			e.db.Set(args[i], args[i+1])
			i += 2
		}
		return entity.MakeStatusReply(NoErr)
	case "mget":
		var values [][]byte
		for i := 1; i < len(args); i++ {
			v, _, err := e.db.Get(args[i])
			if err == datastore.ErrKeyExpired {
				e.db.Del(args[i])
			}
			values = append(values, v)
		}
		return entity.MakeMultiBulkReply(values)
	case "msetnx":
		for i := 1; i < len(args)-1; {
			_, exists, _ := e.db.Get(args[i])
			if exists {
				return entity.MakeErrReply("0")
			}
			i += 2
		}
		for i := 1; i < len(args)-1; {
			e.db.Set(args[i], args[i+1])
			i += 2
		}
		return entity.MakeStatusReply(NoErr)
	case "del":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		var deletedNum int64
		for i := 1; i < len(args); i++ {
			_, exist, _ := e.db.Get(args[i])
			if exist {
				e.db.Del(args[i])
				deletedNum++
			}
		}
		return entity.MakeIntReply(deletedNum)
	case "keys":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		switch string(args[1]) {
		case "*":
			keys := e.db.Keys()
			return entity.MakeMultiBulkReply(keys)
		default:
			return entity.MakeErrReply(ParamNotImplementedErr)
		}
	case "exists":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		_, exists, _ := e.db.Get(args[1])
		if exists {
			return entity.MakeIntReply(1)
		}
		return entity.MakeIntReply(0)
	case "expire":
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		_, exists, _ := e.db.Get(args[1])
		if !exists {
			return entity.MakeIntReply(0)
		}
		n, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		e.db.SetTTL(args[1], n)
		return entity.MakeIntReply(1)
	case "ttl":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		return entity.MakeIntReply(e.db.GetLeftLife(args[1]))
	case "persist":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		_, exist, _ := e.db.Get(args[1])
		if !exist {
			return entity.MakeIntReply(0)
		}
		return entity.MakeIntReply(e.db.Persist(args[1]))
	case "expireat":
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		_, exist, _ := e.db.Get(args[1])
		if !exist {
			return entity.MakeIntReply(0)
		}
		sec, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		e.db.SetDeadLine(args[1], time.Unix(int64(sec), 0))
		return entity.MakeIntReply(1)
	case "lpush": // List
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		values := make([][]byte, len(args)-2)
		for i := 2; i < len(args); i++ {
			values[i-2] = args[i]
		}
		if num := e.db.Lpush(args[1], values); num == -1 {
			return entity.MakeErrReply(datastore.ErrTypeNotMatched.Error())
		} else {
			return entity.MakeIntReply(num)
		}
	case "rpush":
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		values := make([][]byte, len(args)-2)
		for i := 2; i < len(args); i++ {
			values[i-2] = args[i]
		}
		if num := e.db.Rpush(args[1], values); num == -1 {
			return entity.MakeErrReply(datastore.ErrTypeNotMatched.Error())
		} else {
			return entity.MakeIntReply(num)
		}
	case "lrange":
		if len(args) < 4 {
			return entity.MakeErrReply(MissParamErr)
		}
		start, startErr := strconv.Atoi(string(args[2]))
		stop, stopErr := strconv.Atoi(string(args[3]))
		if startErr != nil || stopErr != nil {
			return entity.MakeErrReply(datastore.ErrTypeNotMatched.Error())
		}
		values, err := e.db.Lrange(args[1], start, stop)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		return entity.MakeMultiBulkReply(values)
	case "llen":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		return entity.MakeIntReply(int64(e.db.Llen(args[1])))
	default:
		return entity.MakeErrReply(ParamNotImplementedErr)
	}
}
