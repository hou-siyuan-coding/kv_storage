package list

import (
	"errors"
)

type List struct {
	length     int
	head, tail *ListNode
}

func NewList() *List {
	return &List{}
}

func (l *List) GetLength() int {
	return l.length
}

func (l *List) Lpush(vs [][]byte) {
	for _, v := range vs {
		l.lpush(v)
	}
}

func (l *List) Rpush(vs [][]byte) {
	for _, v := range vs {
		l.rpush(v)
	}
}

func (l *List) lpush(v []byte) {
	if l.head == nil {
		l.head = &ListNode{nodeValue: v}
		l.tail = l.head
	} else {
		newListNode := &ListNode{nodeValue: v}
		newListNode.next = l.head
		l.head.prev = newListNode
		l.head = newListNode
	}
	l.length++
}

func (l *List) rpush(v []byte) {
	if l.head == nil {
		l.head = &ListNode{nodeValue: v}
		l.tail = l.head
	} else {
		newListNode := &ListNode{nodeValue: v}
		newListNode.prev = l.tail
		l.tail.next = newListNode
		l.tail = newListNode
	}
	l.length++
}

func (l *List) Lrange(start, stop int) [][]byte {
	start, stop, err := l.checkborder(start, stop)
	if err != nil {
		return [][]byte{}
	}
	if start >= l.length {
		return [][]byte{}
	}
	if stop >= l.length {
		stop = l.length - 1
	}
	node := l.head
	for i := 0; i < start; node = node.next {
		i++
	}
	vs := make([][]byte, stop-start+1)
	for i := range vs {
		vs[i] = node.nodeValue
		node = node.next
	}
	return vs
}

func (l *List) Lindex(index int) []byte {
	if !l.checkIndex(index) {
		return nil
	}
	node := l.head
	for i := 0; i < index; i++ {
		node = node.next
	}
	return node.nodeValue
}

func (l *List) checkIndex(i int) bool {
	if i < 0 {
		i += l.length
	}
	if i < 0 || i >= l.length {
		return false
	}
	return true
}

func (l *List) Linsert(pivot, value string, before bool) int {
	node := l.head
	if node == nil {
		return 0
	}
	finded := false
	for ; node != nil; node = node.next {
		if string(node.nodeValue) == pivot {
			finded = true
			break
		}
	}
	if !finded {
		return -1
	}
	newNode := &ListNode{nodeValue: []byte(value)}
	if before {
		if node.prev != nil {
			newNode.next = node.prev.next
			node.prev.next = newNode
			newNode.prev = node.prev
			node.prev = newNode
		} else {
			newNode.next = node
			node.prev = newNode
			l.head = newNode
		}
	} else {
		if node.next != nil {
			newNode.next = node.next
			node.next = newNode
			newNode.prev = node
			newNode.next.prev = newNode
		} else {
			node.next = newNode
			newNode.prev = node
			l.tail = newNode
		}
	}
	l.length++
	return l.length
}

func (l *List) remove(p *ListNode) {
	if p.prev == nil { // 待删除节点是表头
		l.head = p.next
		p.next.prev = nil
		p.next = nil
	} else {
		p.prev.next = p.next
		if p.next != nil {
			p.next.prev = p.prev
		} else {
			l.tail = p.prev
		}
		p.prev = nil
		p.next = nil
	}
	l.length--
}

func (l *List) Lrem(count int, value string) int {
	removed := 0
	node := l.head
	if count < 0 { // 从表尾开始删除
		node = l.tail
		for node != nil {
			prevNode := node.prev
			if string(node.nodeValue) == value {
				l.remove(node)
				if removed += 1; removed == (0 - count) {
					return removed
				}
			}
			node = prevNode
		}
	} else { // 从表头开始删除
		for node != nil {
			nextNode := node.next
			if string(node.nodeValue) == value {
				l.remove(node)
				if removed += 1; count != 0 && removed == count {
					return removed
				}
			}
			node = nextNode
		}
	}
	return removed
}

func (l *List) trim(begin, end *ListNode, newLength int) {
	l.length = newLength
	l.head = begin
	if begin.prev != nil {
		begin.prev.next = nil
	}
	begin.prev = nil
	if end == nil {
		return
	}
	l.tail = end
	if end.next != nil {
		end.next.prev = nil
	}
	end.next = nil
}

// 检查索引，索引元素全部为空返回error，否则返回转换后的有效索引
func (l *List) checkborder(start, stop int) (int, int, error) {
	if start < 0 {
		start += l.length
	}
	if stop < 0 {
		stop += l.length
	}
	if start > stop {
		return 0, 0, errors.New("all elements of index are nil")
	}
	if stop < 0 {
		return 0, 0, errors.New("all elements of index are nil")
	}
	if start > l.length-1 {
		return 0, 0, errors.New("all elements of index are nil")
	}
	if start < 0 {
		start = 0
	}
	if stop > l.length-1 {
		stop = l.length - 1
	}
	return start, stop, nil
}

func (l *List) Lset(key string, index int, value string) error {
	if index < 0 {
		index += l.length
	}
	if index < 0 || index >= l.length {
		return errors.New("index outside")
	}
	node := l.head
	for i := 0; i < index; node = node.next {
		i++
	}
	node.nodeValue = []byte(value)
	return nil
}

func (l *List) getHead() []byte {
	if l.head == nil {
		return nil
	}
	if l.head == l.tail {
		l.head = nil
		l.tail = nil
		l.length = 0
		return nil
	}
	node := l.head
	l.head = node.next
	node.next.prev = nil
	node.next = nil
	l.length--
	return node.nodeValue
}

func (l *List) getTail() []byte {
	if l.tail == nil {
		return nil
	}
	if l.head == l.tail {
		l.head = nil
		l.tail = nil
		l.length = 0
		return nil
	}
	node := l.tail
	l.tail = node.prev
	node.prev.next = nil
	node.prev = nil
	l.length--
	return node.nodeValue
}

func (l *List) Lpop(count int) [][]byte {
	size := 0
	if count <= l.length {
		size = count
	} else {
		size = l.length
	}
	poped := make([][]byte, size)
	for i := 0; i < size; i++ {
		poped[i] = l.getHead()
	}
	return poped
}

func (l *List) Rpop(count int) [][]byte {
	size := 0
	if count <= l.length {
		size = count
	} else {
		size = l.length
	}
	poped := make([][]byte, size)
	for i := 0; i < size; i++ {
		poped[i] = l.getTail()
	}
	return poped
}

func (l *List) Ltrim(start, stop int) error {
	start, stop, err := l.checkborder(start, stop)
	if err != nil {
		l.head = nil
		l.tail = nil
		l.length = 0
		return nil
	}
	var begin, end *ListNode
	rank := 0
	for node := l.head; node.next != nil; node = node.next {
		if rank == start {
			begin = node
		}
		if rank == stop {
			end = node
		}
		rank++
	}
	newLength := stop - start + 1
	l.trim(begin, end, newLength)
	return nil
}

type ListNode struct {
	nodeValue  []byte
	next, prev *ListNode
}
