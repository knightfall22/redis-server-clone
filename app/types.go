package main

import (
	"errors"
	"fmt"
	"io"
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

var (
	Lists = make(map[string]*LinkedList)
)

type LinkedList struct {
	Length int
	Head   *Node
	Tail   *Node
}

type Node struct {
	index int
	value string
	next  *Node
}

func (n *Node) getUntil(index int) (out []string) {
	for node := n; node != nil; node = node.next {
		out = append(out, node.value)

		if node.index == index {
			break
		}
	}

	return
}
func (l *LinkedList) Add(values ...string) int {
	for _, v := range values {
		l.add(v)
	}

	l.Length += len(values)
	fmt.Println("VIVI", l.Length)
	return l.Length
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

	return node.getUntil(bottom)
}

func (l *LinkedList) Get(index int) *Node {
	if index >= l.Length {
		index = l.Length - 1
	}

	var resNode *Node
	for node := l.Head; node != nil; node = node.next {
		if node.index == index {
			resNode = node
			break
		}
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

	node.index = l.Tail.index + 1
	l.Tail.next = node
	l.Tail = node
}
