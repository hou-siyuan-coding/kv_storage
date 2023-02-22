package datastore

import (
	"errors"
	"fmt"
	"kv_storage/datastruct/list"
	"kv_storage/datastruct/sortedset"
	"kv_storage/entity"
	"sync"
	"time"
)

var (
	ErrKeyNotExists   = errors.New("key not exists")
	ErrKeyExpired     = errors.New("key expired")
	ErrTypeNotMatched = errors.New("type not matched")
)

type Map struct {
	store map[string]*entity.Value
	mx    sync.Mutex
}

func NewMap() *Map {
	m := make(map[string]*entity.Value, 16)
	return &Map{store: m, mx: sync.Mutex{}}
}

func (m *Map) Set(key []byte, value []byte) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.store[string(key)] = entity.NewValue(value)
}

func (m *Map) Get(key []byte) ([]byte, bool, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return nil, false, ErrKeyNotExists
	}
	if v.Expired() {
		return nil, false, ErrKeyExpired
	}
	vb, ok := v.V.([]byte)
	if !ok {
		return nil, true, ErrTypeNotMatched
	}
	return vb, true, nil
}

func (m *Map) Del(key []byte) {
	m.mx.Lock()
	defer m.mx.Unlock()
	delete(m.store, string(key))
}

func (m *Map) Keys() [][]byte {
	m.mx.Lock()
	defer m.mx.Unlock()
	var keys [][]byte
	for k, v := range m.store {
		if v.Expired() {
			continue
		}
		keys = append(keys, []byte(k))
	}
	return keys
}

func (m *Map) SetTTL(key []byte, second int) {
	m.mx.Lock()
	defer m.mx.Unlock()
	value := m.store[string(key)]
	value.SetTTL(time.Second * time.Duration(second))
}

func (m *Map) SetDeadLine(key []byte, deadLine time.Time) {
	m.mx.Lock()
	defer m.mx.Unlock()
	value := m.store[string(key)]
	value.SetDeadLine(deadLine)
}

func (m *Map) GetLeftLife(key []byte) int64 {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return -2
	}
	if !v.HaveLife() {
		return -1
	}
	return v.GetLeftLife()
}

func (m *Map) Persist(key []byte) int64 {
	m.mx.Lock()
	defer m.mx.Unlock()
	v := m.store[string(key)]
	if !v.HaveLife() {
		return 0
	}
	v.Persist()
	return 1
}

func (m *Map) Lpush(key []byte, values [][]byte) int64 {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		v = &entity.Value{V: list.NewList()}
		m.store[string(key)] = v
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return -1
	}
	list.Lpush(values)
	return int64(len(values))
}

func (m *Map) Rpush(key []byte, values [][]byte) int64 {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		v = &entity.Value{V: list.NewList()}
		m.store[string(key)] = v
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return -1
	}
	list.Rpush(values)
	return int64(len(values))
}

func (m *Map) Lrange(key string, start, stop int) ([][]byte, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return [][]byte{}, nil
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return [][]byte{}, ErrTypeNotMatched
	}
	return list.Lrange(start, stop), nil
}

func (m *Map) Llen(key []byte) int {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return 0
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return 0
	}
	return list.GetLength()
}

func (m *Map) Lindex(key string, index int) ([]byte, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return []byte{}, nil
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return []byte{}, ErrTypeNotMatched
	}
	return list.Lindex(index), nil
}

func (m *Map) Linsert(key string, before bool, pivot, value string) (int, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return 0, nil
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return 0, ErrTypeNotMatched
	}
	return list.Linsert(pivot, value, before), nil
}

func (m *Map) Lrem(key string, count int, value string) (int, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return 0, nil
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return 0, ErrTypeNotMatched
	}
	return list.Lrem(count, value), nil
}

func (m *Map) Ltrim(key string, start, stop int) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return nil
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return ErrTypeNotMatched
	}
	return list.Ltrim(start, stop)
}

func (m *Map) Lset(key string, index int, value string) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return errors.New("key not exist")
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return ErrTypeNotMatched
	}
	return list.Lset(key, index, value)
}

func (m *Map) Lpop(key string, count int) ([][]byte, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return nil, nil
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return nil, ErrTypeNotMatched
	}
	return list.Lpop(count), nil
}

func (m *Map) Rpop(key string, count int) ([][]byte, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return nil, nil
	}
	list, ok := v.V.(*list.List)
	if !ok {
		return nil, ErrTypeNotMatched
	}
	return list.Rpop(count), nil
}

// sortedset
func (m *Map) Zadd(scores []float64, names []string, key string) (int, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	zset, exist := m.store[key]
	if !exist {
		zset = entity.NewValue(sortedset.Make())
		m.store[key] = zset
	}
	sortedSet, ok := zset.V.(*sortedset.SortedSet)
	if !ok {
		return 0, ErrTypeNotMatched
	}
	insertedNum := 0
	for i, name := range names {
		if inserted := sortedSet.Add(name, scores[i]); inserted {
			insertedNum++
		}
	}
	return insertedNum, nil
}

func (m *Map) Zrange(key string, start, stop int, desc, withScore bool) ([][]byte, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	zset, exist := m.store[key]
	if !exist {
		return [][]byte{}, nil
	}
	sortedSet, ok := zset.V.(*sortedset.SortedSet)
	if !ok {
		return nil, ErrTypeNotMatched
	}
	elems := sortedSet.Range(int64(start), int64(stop), desc)
	if elems == nil {
		return [][]byte{}, nil
	}
	size := len(elems)
	if withScore {
		size *= 2
	}
	rs := make([][]byte, size)
	i := 0
	for _, elem := range elems {
		rs[i] = []byte(elem.Member)
		if withScore {
			rs[i+1] = []byte(fmt.Sprintf("%v", elem.Score))
			i += 2
		} else {
			i++
		}
	}
	return rs, nil
}

func (m *Map) ZrangeByScore(key string, min, max *sortedset.ScoreBorder, withScore, desc bool, offset, count int) ([][]byte, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	zset, exist := m.store[key]
	if !exist {
		return [][]byte{}, nil
	}
	sortedSet, ok := zset.V.(*sortedset.SortedSet)
	if !ok {
		return nil, ErrTypeNotMatched
	}
	elems := sortedSet.RangeByScore(min, max, int64(offset), int64(count), desc)
	if elems == nil {
		return [][]byte{}, nil
	}
	size := len(elems)
	if withScore {
		size *= 2
	}
	rs := make([][]byte, size)
	i := 0
	for _, elem := range elems {
		rs[i] = []byte(elem.Member)
		if withScore {
			rs[i+1] = []byte(fmt.Sprintf("%v", elem.Score))
			i += 2
		} else {
			i++
		}
	}
	return rs, nil
}

func (m *Map) Zcard(key string) (int64, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	zset, exist := m.store[key]
	if !exist {
		return 0, nil
	}
	sortedSet, ok := zset.V.(*sortedset.SortedSet)
	if !ok {
		return 0, ErrTypeNotMatched
	}
	return sortedSet.Len(), nil
}

func (m *Map) Zrem(key string, names [][]byte) (int64, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	zset, exist := m.store[key]
	if !exist {
		return 0, nil
	}
	sortedSet, ok := zset.V.(*sortedset.SortedSet)
	if !ok {
		return 0, ErrTypeNotMatched
	}
	var removedNum int64
	for _, v := range names {
		if removed := sortedSet.Remove(string(v)); removed {
			removedNum++
		}
	}
	return removedNum, nil
}

func (m *Map) Zcount(key string, min, max []byte) (int64, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	zset, exist := m.store[key]
	if !exist {
		return 0, nil
	}
	sortedSet, ok := zset.V.(*sortedset.SortedSet)
	if !ok {
		return 0, ErrTypeNotMatched
	}
	minBorder, err := sortedset.ParseScoreBorder(string(min))
	if err != nil {
		return 0, err
	}
	maxBorder, err := sortedset.ParseScoreBorder(string(max))
	if err != nil {
		return 0, err
	}
	return sortedSet.Count(minBorder, maxBorder), nil
}

func (m *Map) Zrank(key, name []byte, desc bool) (int64, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	zset, exist := m.store[string(key)]
	if !exist {
		return 0, nil
	}
	sortedSet, ok := zset.V.(*sortedset.SortedSet)
	if !ok {
		return 0, ErrTypeNotMatched
	}
	return sortedSet.GetRank(string(name), desc), nil
}
