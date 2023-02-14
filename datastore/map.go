package datastore

import (
	"errors"
	"kv_storage/datastruct"
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
		v = &entity.Value{V: datastruct.NewList()}
		m.store[string(key)] = v
	}
	list, ok := v.V.(*datastruct.List)
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
		v = &entity.Value{V: datastruct.NewList()}
		m.store[string(key)] = v
	}
	list, ok := v.V.(*datastruct.List)
	if !ok {
		return -1
	}
	list.Rpush(values)
	return int64(len(values))
}

func (m *Map) Lrange(key []byte, start, stop int) ([][]byte, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, exist := m.store[string(key)]
	if !exist {
		return [][]byte{}, nil
	}
	list, ok := v.V.(*datastruct.List)
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
	list, ok := v.V.(*datastruct.List)
	if !ok {
		return 0
	}
	return list.GetLength()
}
