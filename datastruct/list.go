package datastruct

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
	if start >= l.length {
		return [][]byte{}
	}
	if stop >= l.length {
		stop = l.length - 1
	}
	currentNum := 0
	node := l.head
	for ; currentNum < start; node = node.next {
		currentNum++
	}
	rangeNum := l.length + stop
	vs := make([][]byte, rangeNum-currentNum+1)
	for i := 0; currentNum <= rangeNum; node = node.next {
		vs[i] = node.nodeValue
		i++
		currentNum++
	}
	return vs
}

type ListNode struct {
	nodeValue  []byte
	next, prev *ListNode
}
