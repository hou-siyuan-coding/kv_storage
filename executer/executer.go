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
	ParamUncorrect         = "args uncorrect"
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
	case "ping":
		return entity.MakeBulkReply([]byte("pong"))
	case "set": // string
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
	case "del": // key
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
		values, err := prePush(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if num := e.db.Lpush(args[1], values); num == -1 {
			return entity.MakeErrReply(datastore.ErrTypeNotMatched.Error())
		} else {
			return entity.MakeIntReply(num)
		}
	case "rpush":
		values, err := prePush(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if num := e.db.Rpush(args[1], values); num == -1 {
			return entity.MakeErrReply(datastore.ErrTypeNotMatched.Error())
		} else {
			return entity.MakeIntReply(num)
		}
	case "lrange":
		key, start, stop, err := preLrange(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		values, err := e.db.Lrange(key, start, stop)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		return entity.MakeMultiBulkReply(values)
	case "llen":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		return entity.MakeIntReply(int64(e.db.Llen(args[1])))
	case "lindex":
		key, index, err := preLindex(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if value, err := e.db.Lindex(key, index); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeBulkReply(value)
		}
	case "linsert":
		key, before, pivot, value, err := preLinsert(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if length, err := e.db.Linsert(key, before, pivot, value); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(int64(length))
		}
	case "lrem":
		key, count, value, err := preLrem(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if removedNum, err := e.db.Lrem(key, count, value); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(int64(removedNum))
		}
	case "ltrim":
		key, start, stop, err := preLrange(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if err := e.db.Ltrim(key, start, stop); err != nil {
			return entity.MakeErrReply(err.Error())
		}
		return entity.MakeOkReply()
	case "lset":
		key, index, value, err := preLset(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if e.db.Lset(key, index, value); err != nil {
			return entity.MakeErrReply(err.Error())
		}
		return entity.MakeOkReply()
	case "lpop":
		key, count, err := prePop(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if poped, err := e.db.Lpop(key, count); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeMultiBulkReply(poped)
		}
	case "rpop":
		key, count, err := prePop(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if poped, err := e.db.Rpop(key, count); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeMultiBulkReply(poped)
		}
	case "zadd": // zset
		names, scores, err := preZadd(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if insertedNum, err := e.db.Zadd(scores, names, string(args[1])); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(int64(insertedNum))
		}
	case "zrange":
		start, stop, withScore, err := preZrevrange(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if rs, err := e.db.Zrange(string(args[1]), start, stop, false, withScore); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeMultiBulkReply(rs)
		}
	case "zrevrange":
		start, stop, withScore, err := preZrevrange(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if rs, err := e.db.Zrange(string(args[1]), start, stop, true, withScore); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeMultiBulkReply(rs)
		}
	case "zrem":
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		if removedNum, err := e.db.Zrem(string(args[1]), args[2:]); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(removedNum)
		}
	case "zcard":
		if len(args) < 2 {
			return entity.MakeErrReply(MissParamErr)
		}
		if num, err := e.db.Zcard(string(args[1])); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(num)
		}
	case "zcount":
		if len(args) < 4 {
			return entity.MakeErrReply(MissParamErr)
		}
		if num, err := e.db.Zcount(string(args[1]), args[2], args[3]); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(num)
		}
	case "zrangebyscore":
		key, min, max, withScore, offset, count, err := preZrankByScore(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if rs, err := e.db.ZrangeByScore(key, min, max, withScore, false, offset, count); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeMultiBulkReply(rs)
		}
	case "zrevrangebyscore":
		key, min, max, withScore, offset, count, err := preZrankByScore(args)
		if err != nil {
			return entity.MakeErrReply(err.Error())
		}
		if rs, err := e.db.ZrangeByScore(key, min, max, withScore, true, offset, count); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeMultiBulkReply(rs)
		}
	case "zrank":
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		if rank, err := e.db.Zrank(args[1], args[2], false); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(rank)
		}
	case "zrevrank":
		if len(args) < 3 {
			return entity.MakeErrReply(MissParamErr)
		}
		if rank, err := e.db.Zrank(args[1], args[2], true); err != nil {
			return entity.MakeErrReply(err.Error())
		} else {
			return entity.MakeIntReply(rank)
		}
	// case "zincrby":
	default:
		return entity.MakeErrReply(ParamNotImplementedErr)
	}
}
