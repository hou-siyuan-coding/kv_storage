package main

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

func main() {
	// zset := sortedset.MakeSkiplist()
	// ns := []float64{0, 2, 5, 1, 3, 0, 6, -1, 11, 7, 10, 8, 14, 9, 12, 13, 5}
	// for i, v := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n",
	// 	"o", "p", "q"} {
	// 	zset.Insert(v, ns[i])
	// }
	l := NewList()
	l.Rpush([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")})
}
