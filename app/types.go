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
	SortedMu  = sync.Mutex{}
	SortedSet = make(map[string]*SkipListSortedSet)
)

type ListValue struct {
	name  string
	score float64
}

type SkipListNode struct {
	value ListValue
	next  []*SkipListNode
}

type SkipListSortedSet struct {
	head     *SkipListNode
	maxlevel int
	size     int
	rand     *rand.Rand
}

const MaxLevel = 15

func NewSkipListSortedSet() *SkipListSortedSet {
	// Create head node with sentinel value
	head := &SkipListNode{
		value: ListValue{
			score: -1 << 31,
		},
		next: make([]*SkipListNode, MaxLevel+1),
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

func (s *SkipListSortedSet) Add(val ListValue) int {
	update := make([]*SkipListNode, MaxLevel+1)
	current := s.head

	for level := s.maxlevel; level >= 0; level-- {
		for current.next[level] != nil && bytes.Compare([]byte(val.name), []byte(current.next[level].value.name)) == 1 {
			current = current.next[level]
		}

		update[level] = current
	}

	current = current.next[0]

	// Check if value already exists
	if current != nil &&
		current.value.name == val.name {
		current.value.score = val.score

		return 0
	}

	// Generate random level for new node
	newLevel := s.randomLevel()

	// If new level is higher than current max, update head pointers
	if newLevel > s.maxlevel {
		for i := s.maxlevel + 1; i <= newLevel; i++ {
			update[i] = s.head
		}

		s.maxlevel = newLevel
	}

	// Create new node
	newNode := &SkipListNode{
		value: val,
		next:  make([]*SkipListNode, newLevel+1),
	}

	// Insert the node at all levels up to its random level
	for i := 0; i <= newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	s.size++
	return 1
}

func (s *SkipListSortedSet) Remove(name string) bool {
	update := make([]*SkipListNode, MaxLevel+1)
	current := s.head

	for level := s.maxlevel; level >= 0; level-- {
		for current.next[level] != nil && bytes.Compare([]byte(name), []byte(current.next[level].value.name)) == 1 {
			current = current.next[level]
		}

		update[level] = current
	}

	current = current.next[0]

	if current == nil || current.value.name != name {
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
