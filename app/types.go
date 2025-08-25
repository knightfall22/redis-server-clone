package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

var signalSearch = make(chan []byte)
var available = make(chan []byte)

var ERRInvalidCommand = errors.New("ERR invalid command")

var (
	connections []io.Writer
	connMu      sync.Mutex
)

type setVal struct {
	value   string
	timeout *time.Time
}

var (
	SETs   = map[string]setVal{}
	SETsMu = sync.RWMutex{}
)

type MapKVs struct {
	Key   string
	Value string
}

type WatchEvent struct {
	stream string
	id     string
}

var (
	stream    = map[string]*iradix.Tree[[]MapKVs]{}
	watchers  = make(map[string]map[string][]chan WatchEvent)
	streamMu  = sync.RWMutex{}
	topStream = make(map[string]string)
)

type ListWatchReceiver struct {
	ch          chan string
	timeStarted time.Time
}

var (
	ListMu      = sync.RWMutex{}
	ListWatcher = make(map[string][]ListWatchReceiver)
	Lists       = make(map[string]*LinkedList)
)
var (
	offset   = 0
	offsetMu sync.Mutex
)

const (
	STRING  = '+'
	ERROR   = '-'
	ARRAY   = '*'
	MAP     = '%'
	BULK    = '$'
	INTEGER = ':'
)

type Value struct {
	typ      string
	str      string
	integer  int
	len      int
	bulk     string
	array    []Value
	contents []byte
}

type Node struct {
	value string
	next  *Node
}

func (n *Node) getUntil(count int) (out []string) {
	i := 0
	for node := n; node != nil; node = node.next {
		out = append(out, node.value)

		if i == count {
			break
		}
		i++
	}

	return
}

type LinkedList struct {
	Length int
	Head   *Node
	Tail   *Node
}

func (l *LinkedList) Add(values ...string) int {
	for _, v := range values {
		l.add(v)
	}

	l.Length += len(values)

	return l.Length
}

func (l *LinkedList) RAdd(values ...string) int {
	for _, v := range values {
		l.radd(v)
	}

	l.Length += len(values)

	return l.Length
}

func (l *LinkedList) radd(value string) {
	node := &Node{
		value: value,
	}

	//If head and tail are empty add new node to them
	if l.Head == nil && l.Tail == nil {
		l.Head = node
		l.Tail = node
		fmt.Println(l.Head.value)
		return
	}

	l.Tail = l.Head
	l.Head = node
	l.Head.next = l.Tail
	fmt.Println(l.Head.next.value)
}

func (l *LinkedList) Pop() string {
	if l.Head == nil {
		return ""
	}

	val := l.Head.value

	l.Head = l.Head.next

	return val
}

func (l *LinkedList) Range(top, bottom int) []string {

	if top < 0 {
		top = max(l.Length+top, 0)
	} else if top > l.Length {
		return nil
	}

	if bottom < 0 {
		bottom = max(l.Length+bottom, 0)

	}

	if top > bottom {
		return nil
	}

	node := l.Get(top)

	return node.getUntil(bottom - top)
}

func (l *LinkedList) Get(index int) *Node {
	if index >= l.Length {
		index = l.Length - 1
	}

	var resNode *Node
	i := 0
	for node := l.Head; node != nil; node = node.next {
		resNode = node

		if index == i {
			break
		}
		i++
	}

	return resNode
}

func (l *LinkedList) add(value string) {
	node := &Node{
		value: value,
	}

	//If head and tail are empty add new node to them
	if l.Head == nil && l.Tail == nil {
		l.Head = node
		l.Tail = node
		return
	}

	l.Tail.next = node
	l.Tail = node
}

// //////////////////////////////////////
// /SORT SETS///////////////////////////
// ////////////////////////////////////
var (
	SortedMu     = sync.Mutex{}
	SortedSetMap = make(map[string]ListValue)
	SortedSet    = make(map[string]*SkipListSortedSet)
)

type ListValue struct {
	name  string
	score float64
}

type SkipListNode struct {
	value ListValue
	next  []*SkipListNode
	span  []int
}

type SkipListSortedSet struct {
	head     *SkipListNode
	maxlevel int
	size     int
	rand     *rand.Rand
}

const MaxLevel = 15

func AddToSortedList(key string, val ListValue) int {
	SortedMu.Lock()
	defer SortedMu.Unlock()

	if SortedSet[key] == nil {
		SortedSet[key] = NewSkipListSortedSet()
	}

	ssMapKay := fmt.Sprintf("%s:%s", key, val.name)
	if _, ok := SortedSetMap[ssMapKay]; !ok {
		SortedSetMap[ssMapKay] = ListValue{
			name:  val.name,
			score: val.score,
		}

		return SortedSet[key].add(val)
	}

	//pullout old value
	oldVal := SortedSetMap[ssMapKay]
	SortedSet[key].Remove(oldVal)

	SortedSetMap[ssMapKay] = ListValue{
		name:  val.name,
		score: val.score,
	}
	SortedSet[key].add(val)

	return 0
}

func GetRank(key, name string) int {
	SortedMu.Lock()
	defer SortedMu.Unlock()

	ssMapKay := fmt.Sprintf("%s:%s", key, name)
	item, ok := SortedSetMap[ssMapKay]
	if !ok {
		return -1
	}

	if SortedSet[key] == nil {
		SortedSet[key] = NewSkipListSortedSet()
	}

	fmt.Println(SortedSet[key].ToSlice())
	return SortedSet[key].rank(
		ListValue{
			name:  name,
			score: item.score,
		},
	)

}

func GetRange(key string, start, stop int) []ListValue {
	SortedMu.Lock()
	defer SortedMu.Unlock()

	if SortedSet[key] == nil {
		return nil
	}

	return SortedSet[key].zrange(start, stop)

}

func GetCard(key string) int {
	SortedMu.Lock()
	defer SortedMu.Unlock()

	if SortedSet[key] == nil {
		return 0
	}

	return SortedSet[key].size
}

func GetScore(key, name string) string {
	ssMapKay := fmt.Sprintf("%s:%s", key, name)
	item, ok := SortedSetMap[ssMapKay]
	if !ok {
		return ""
	}

	fmt.Printf("Sorted SET MAP %+v", SortedSetMap)

	return fmt.Sprintf("%.10f", item.score)
}

func NewSkipListSortedSet() *SkipListSortedSet {
	// Create head node with sentinel value
	head := &SkipListNode{
		value: ListValue{
			score: -1 << 31,
		},
		next: make([]*SkipListNode, MaxLevel+1),
		span: make([]int, MaxLevel+1),
	}

	return &SkipListSortedSet{
		head:     head,
		maxlevel: 0,
		size:     0,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *SkipListSortedSet) randomLevel() int {
	level := 0

	if s.rand.Float64() < 0.5 && level < MaxLevel {
		level++
	}

	return level
}

func compare(a, b ListValue) int {
	if a.score < b.score {
		return -1
	}
	if a.score > b.score {
		return 1
	}
	// Same score, compare by value lexicographically
	if bytes.Compare([]byte(a.name), []byte(b.name)) == -1 {
		return -1
	}
	if bytes.Compare([]byte(a.name), []byte(b.name)) == 1 {
		return 1
	}
	return 0 // Equal
}

func (s *SkipListSortedSet) add(val ListValue) int {
	update := make([]*SkipListNode, MaxLevel+1)
	rank := make([]int, MaxLevel+1) // Track ranks during search
	current := s.head

	for level := s.maxlevel; level >= 0; level-- {
		if level != s.maxlevel {
			rank[level] = rank[level+1]
		}

		for current.next[level] != nil && compare(current.next[level].value, val) < 0 {
			rank[level] += current.span[level]
			current = current.next[level]
		}

		update[level] = current
	}

	// Generate random level for new node
	newLevel := s.randomLevel()

	// If new level is higher than current max, update head pointers
	if newLevel > s.maxlevel {
		for i := s.maxlevel + 1; i <= newLevel; i++ {
			rank[i] = 0
			update[i] = s.head
			update[i].span[i] = s.size
		}

		s.maxlevel = newLevel
	}

	// Create new node
	newNode := &SkipListNode{
		value: val,
		next:  make([]*SkipListNode, newLevel+1),
		span:  make([]int, newLevel+1),
	}

	// Insert the node at all levels up to its random level
	for i := 0; i <= newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode

		//update spans
		newNode.span[i] = update[i].span[i] - (rank[0] - rank[i])
		update[i].span[i] = (rank[0] - rank[i]) + 1
	}

	// Update spans for levels not touched by new node
	for i := newLevel + 1; i <= s.maxlevel; i++ {
		update[i].span[i]++
	}

	s.size++
	return 1
}

func (s *SkipListSortedSet) zrange(start, stop int) []ListValue {
	if start < 0 {
		start = max(s.size+start, 0)
	}

	if stop < 0 {
		stop = max(s.size+stop, 0)
	}

	if start > s.size || start > stop {
		return nil
	}

	if stop > s.size {
		stop = s.size
	}

	current := s.head.next[0]

	for i := 0; i < start && current != nil; i++ {
		current = current.next[0]
	}

	var result []ListValue

	for i := 0; i <= stop && current != nil; i++ {
		result = append(result, current.value)
		current = current.next[0]
	}

	return result
}

// func (s *SkipListSortedSet) retrieve(val ListValue) string {
// 	current := s.head

// 	for level := s.maxlevel; level >= 0; level-- {
// 		for current.next[level] != nil && compare(current.next[level].value, val) < 0 {
// 			current = current.next[level]
// 		}
// 	}

// 	current = current.next[0]

// 	if current != nil && current.value.name == val.name {
// 		return current.value.name
// 	}

// 	return ""
// }

func (s *SkipListSortedSet) rank(val ListValue) int {
	current := s.head
	rank := 0

	for level := s.maxlevel; level >= 0; level-- {
		for current.next[level] != nil && compare(current.next[level].value, val) < 0 {
			rank += current.span[level]
			current = current.next[level]
		}
	}

	current = current.next[0]

	if current != nil && current.value.name == val.name {
		return rank
	}
	return -1
}

func (s *SkipListSortedSet) Remove(val ListValue) bool {
	update := make([]*SkipListNode, MaxLevel+1)
	current := s.head

	for level := s.maxlevel; level >= 0; level-- {
		for current.next[level] != nil && compare(current.next[level].value, val) < 0 {
			current = current.next[level]
		}

		update[level] = current
	}

	current = current.next[0]

	if current == nil || current.value.name != val.name || current.value.score != val.score {
		return false
	}

	for i := 0; i <= s.maxlevel; i++ {
		if update[i].next[i] != current {
			break
		}

		update[i].next[i] = current.next[i]
	}

	// Update max level if necessary
	for s.maxlevel > 0 && s.head.next[s.maxlevel] == nil {
		s.maxlevel--
	}

	s.size--
	return true
}

func (s *SkipListSortedSet) ToSlice() (result []ListValue) {
	current := s.head.next[0]

	for current != nil {
		result = append(result, current.value)
		current = current.next[0]
	}
	return
}
