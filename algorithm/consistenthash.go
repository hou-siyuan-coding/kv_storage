package algorithm

/* 一致性哈希算法主要解决把数据平均分配到多个节点上的问题，并且某些节点上线或者下线后依然能够做到自动负载均衡。
 * 原理是抽象出一个哈希环，把节点ID通过哈希函数映射到环上面。
 * 把处理的数据也通过哈希函数映射到环上，然后顺时针查找，遇到的第一个服务器节点用于处理该数据。
 * 但是由于哈希函数的随机性，可能会出现某些节点负责的哈希环很长，而其他节点很短的情况。
 * 而且随着节点的下线，该节点负责的数据会顺时针转移到下一个节点上，造成数据分布不均匀，数据倾斜。
 * 解决办法是把一个物理服务器节点映射为多个虚拟节点落到哈希环上，由于哈希函数的随机性，
 * 每个真实节点的虚拟节点能够均匀分布在环上，那么数据就可以比较均匀的分布到所有的真实节点上。
 * 而且伴随真实节点下线，该节点所对应的所有虚拟节点都会下线，而每个虚拟节点顺时针方向的下一个节点大概率不会是同一个真实节点，
 * 所以节点下线后也可以让其他真实节点均匀分担下线节点的数据。
 */

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

var Consistenthash = New(4, nil)

// HashFunc defines function to generate hash code
type HashFunc func(data []byte) uint32

type Map struct {
	hashFunc      HashFunc
	replicas      int            // 每个真实节点会产生的虚拟节点个数
	keys          []int          // sorted哈希环
	virtualToReal map[int]string // 虚拟节点的哈希值都真实节点的映射
}

func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas:      replicas,
		hashFunc:      fn,
		virtualToReal: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *Map) GetKeys() []int {
	return m.keys
}

func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			// 使用 i + key 作为一个虚拟节点，计算虚拟节点的 hash 值
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			// 将虚拟节点添加到环上
			m.keys = append(m.keys, hash)
			// 注册虚拟节点到物理节点的映射
			m.virtualToReal[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// support hash tag
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

// 返回数据后面的的首个真实节点
func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))

	// 返回数据后面的的首个虚拟节点
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// 哈希环
	if idx == len(m.keys) {
		idx = 0
	}

	return m.virtualToReal[m.keys[idx]]
}
