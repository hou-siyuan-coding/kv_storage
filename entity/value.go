package entity

import (
	"kv_storage/ttl"
	"time"
)

type Value struct {
	V   interface{}
	ttl *ttl.TTL
}

func NewValue(v interface{}) *Value {
	return &Value{V: v}
}

func NewValueWithTTL(v interface{}, lifeCycle time.Duration) *Value {
	return &Value{V: v, ttl: ttl.NewTTL(lifeCycle)}
}

func (v *Value) Expired() bool {
	if v.ttl == nil {
		return false
	}
	return time.Now().After(v.ttl.DeadLine)
}

func (v *Value) SetTTL(lifeCycle time.Duration) {
	v.ttl = ttl.NewTTL(lifeCycle)
}

func (v *Value) SetDeadLine(deadLine time.Time) {
	v.ttl = ttl.NewTTLWithDeadLine(deadLine)
}

func (v *Value) HaveLife() bool {
	return v.ttl != nil
}

func (v *Value) GetLeftLife() int64 {
	return int64(time.Until(v.ttl.DeadLine) / time.Second)
}

func (v *Value) Persist() {
	v.ttl = nil
}
