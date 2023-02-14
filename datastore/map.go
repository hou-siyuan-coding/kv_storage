package datastore

import (
	"errors"
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