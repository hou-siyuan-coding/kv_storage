package sortedset

import (
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 16 // 最大层数
)

// 跳表是在有序链表的基础之上，构建了多层查找索引，每一层索引都是一个节点数更少的链表，并通过随机函数动态维护索引。
// 跳表的查找、插入和删除操作的时间复杂度为O(log(n))、空间复杂度为O(n)。
// 是可以进行二分查找的有序链表。
type skiplist struct {
	header *node
	tail   *node
	length int64 // 元素个数
	level  int16 // 链表层数
}

// 节点存储的数据
type Element struct {
	Member string
	Score  float64
}

// 连接同层级节点
type Level struct {
	forward *node // 后继指针
	span    int64 // 对于底层链表来说，后继节点是当前节点后面的第几个节点
}

type node struct {
	Element
	backward *node    // 前驱指针，底层双向链表
	level    []*Level // 节点所在层级，level[0]为底层链表，0、1、2...层级越高节点越少
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k)) + 1
}

func (skiplist *skiplist) insert(member string, score float64) *node {
	update := make([]*node, maxLevel) // 每一层中待插入节点的前驱节点
	rank := make([]int64, maxLevel)   // 每一层中前驱节点在底层链表的排名，排名从0开始

	// 找到各层的前驱节点和相对于底层链表的排名
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		if i == skiplist.level-1 {
			// 顶层索引排名从0开始
			rank[i] = 0
		} else {
			// 非顶层索引排名从上层索引开始
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	// 随机函数生成链表层数level，在底层链表至level层中的每一层都插入新节点
	// 随机函数需要按照指定概率生成level，每一个索引层中的节点个数应该是下层节点数的1/2
	// 需保证生成1的概率是1/2、2是1/4、3是1/8，并且抽取到上层索引的元素值均匀，才能实现时间复杂度为O(log(n))的查找操作
	level := randomLevel()

	if level > skiplist.level { // 构建新的索引层
		for i := skiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// 自底向上逐层插入新节点
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		// 更新节点跨度，rank[0]-rank[1]为底层前驱节点和当前层前驱节点之间的节点跨度
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 未触达的索引层，需要在每层中的前驱节点到下一个节点的跨度+1
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// 更新节点的前驱指针
	if update[0] == skiplist.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skiplist.tail = node
	}
	skiplist.length++
	return node
}

/*
 * param node: 待删除节点
 * param update: 各层待删除节点的前驱节点
 */
func (skiplist *skiplist) removeNode(node *node, update []*node) {
	for i := int16(0); i < skiplist.level; i++ {
		if update[i].level[i].forward == node {
			// node存在的层级
			update[i].level[i].span += node.level[i].span - 1// 更新跨度 
			update[i].level[i].forward = node.level[i].forward // 更新前驱节点的后继指针
		} else {
			// node不存在的层级
			update[i].level[i].span--
		}
	}
	// 更新后继节点的前驱指针
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		skiplist.tail = node.backward
	}
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		// 删除了顶层索引的最后一个节点，只剩下头节点，减少索引层
		skiplist.level--
	}
	skiplist.length--
}

func (skiplist *skiplist) remove(member string, score float64) bool {
	update := make([]*node, maxLevel)// 各层待删除节点的前驱节点
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward// 待删除节点
	if node != nil && score == node.Score && node.Member == member {
		skiplist.removeNode(node, update)
		return true
	}
	return false
}

// 下标从1开始
func (skiplist *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		if x.Member == member {
			return rank
		}
	}
	return 0
}

// 下标从1开始
func (skiplist *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

// 判断min和max构成的范围是否是一个有效范围
func (skiplist *skiplist) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}
	// min > tail
	n := skiplist.tail
	if n == nil || !min.less(n.Score) {
		return false
	}
	// max < head
	n = skiplist.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}
	return true
}

// 返回分数范围内的最小分数节点
func (skiplist *skiplist) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

// 返回分数范围内的最大分数节点
func (skiplist *skiplist) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	if !min.less(n.Score) {
		return nil
	}
	return n
}

// 在min和max范围内删除limit个元素，从左向右删除
func (skiplist *skiplist) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder, limit int) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)
	// find backward nodes (of target range) or last node of each level
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(node.level[i].forward.Score) { // already in range
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	// node is the first one within range
	node = node.level[0].forward

	// remove nodes in range
	for node != nil {
		if !max.greater(node.Score) { // already out of range
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

// 删除排名在[start, stop)中的元素并返回，排名从1开始
func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0 // rank of iterator
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	// scan from top level
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward // first node in range

	// remove nodes in range
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
