package main

import (
	"fmt"
	"kv_storage/datastruct"
	"math/rand"
)

func main() {
	m := make(map[int]int)
	for i := 0; i < 100; i++ {
		n := datastruct.RandomLevel()
		if v, ok := m[n]; ok {
			m[n] = v + 1
		} else {
			m[n] = 1
		}
	}
	fmt.Println(m)
}

func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < 16 {
		return level
	}
	return 16
}
