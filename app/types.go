package main

import (
	"errors"
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

func (l *LinkedList) Add(values ...string) int {
	for _, v := range values {
		l.add(v)
	}

	l.Length += len(values)
	return l.Length
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

type Node struct {
	value string
	next  *Node
}
