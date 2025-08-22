package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		fmt.Println("From", err)
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case STRING:
		return r.readString()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

// Read line up to new line.
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			fmt.Println(err)
			return nil, 0, err
		}

		n++
		line = append(line, b)

		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		fmt.Println(err)
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		fmt.Println(err)
		return 0, 0, err
	}

	return int(i64), n, nil
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	//read the length of the array
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	//foreach line, parse and read the value
	v.array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		v.array[i] = val
	}
	return v, nil

}
func (r *Resp) readString() (Value, error) {
	v := Value{}
	v.typ = "string"

	//read string
	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	v.str = string(line)
	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	//read the length of the string
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, length)

	_, err = r.reader.Read(bulk) //Read bytes up to length
	if err != nil {
		return v, err
	}

	v.bulk = string(bulk)

	next, err := r.reader.Peek(2)
	if err != nil {
		return v, err
	}

	// could not be a bulk
	if next[0] != '\r' && next[1] != '\n' {
		return v, nil
	}
	// Read the trailing CRLF
	r.readLine()

	return v, nil
}

// Writing Resp

type Writer struct {
	writer      io.Writer
	queue       []Value
	transaction bool
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w, transaction: false, queue: make([]Value, 0)}
}

func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// Convert response to bytes representing the response RESP
func (v *Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "map":
		return v.marshallMap()
	case "integer":
		return v.marshallInteger()
	case "file":
		return v.marshalFile()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}

func (v *Value) marshalArray() (bytes []byte) {
	len := len(v.array)
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

func (v *Value) marshalBulk() (bytes []byte) {
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v *Value) marshalString() (bytes []byte) {
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v *Value) marshalError() (bytes []byte) {
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v *Value) marshallMap() (bytes []byte) {
	len := len(v.array)
	bytes = append(bytes, MAP)
	bytes = append(bytes, strconv.Itoa(len/2)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i += 2 {
		bytes = append(bytes, v.array[i].Marshal()...)
		bytes = append(bytes, v.array[i+1].Marshal()...)
	}

	return bytes
}

func (v *Value) marshalFile() (bytes []byte) {
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(v.len)...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.contents...)
	return bytes
}

func (v *Value) marshallInteger() (bytes []byte) {
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, strconv.Itoa(v.integer)...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v *Value) marshalNull() []byte {
	//Default
	return []byte("$-1\r\n")
}

// commands handler
func (w *Writer) Handler(v Value) error {
	command := strings.ToUpper(v.array[0].bulk)
	args := v.array[1:]

	switch command {
	case "SET":
		return w.Write(w.set(v, args))
	case "GET":
		return w.Write(w.get(v, args))
	case "RPUSH":
		return w.Write(w.rpush(v, args))
	case "LPUSH":
		return w.Write(w.lpush(v, args))
	case "LLEN":
		return w.Write(w.llen(v, args))
	case "LRANGE":
		return w.Write(w.lrange(v, args))
	case "LPOP":
		return w.Write(w.lpop(v, args))
	case "BLPOP":
		return w.Write(w.lbop(v, args))
	case "ZADD":
		return w.Write(w.zadd(v, args))
	case "PSYNC":
		return w.psync(v, args)
	case "TYPE":
		return w.Write(w.typeIdent(v, args))
	case "PING":
		return w.Write(w.ping(v, args))
	case "REPLCONF":
		return w.Write(w.replconf(v, args))
	case "INFO":
		return w.Write(w.info(v, args))
	case "WAIT":
		return w.Write(w.wait(v, args))
	case "KEYS":
		return w.Write(w.keys(v, args))
	case "CONFIG":
		return w.Write(w.config(v, args))
	case "ECHO":
		return w.Write(w.echo(v, args))
	//Transactions:
	case "INCR":
		return w.Write(w.incr(v, args))
	case "MULTI":
		return w.Write(w.multi(args))
	case "EXEC":
		return w.Write(w.exec(args))
	case "DISCARD":
		return w.Write(w.discard(args))
	//Stream:
	case "XADD":
		return w.Write(w.xAdd(v, args))
	case "XRANGE":
		return w.Write(w.xrange(v, args))
	case "XREAD":
		return w.Write(w.xread(v, args))
	default:
		return w.Write(Value{typ: "string", str: ""})
	}
}

func (w *Writer) HandleSlave(v Value) error {
	command := strings.ToUpper(v.array[0].bulk)
	args := v.array[1:]

	var err error

	switch command {
	case "PING":
	case "SET":
		w.set(v, args)
	case "REPLCONF":
		err = w.Write(w.replconf(v, args))
	default:
		err = w.Write(Value{typ: "string", str: ""})
	}

	valCopy := v
	valcount := len(valCopy.Marshal())
	offsetMu.Lock()
	offset += valcount
	offsetMu.Unlock()

	return err
}

func (w *Writer) info(v Value, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'info'  command"}
	}

	command := strings.ToUpper(args[0].bulk)

	switch command {
	case "REPLICATION":
		id := ConfigMap["masterID"]
		offset := ConfigMap["masterOffset"]

		var strOut string
		if ConfigMap.IsSlave() {
			strOut += "role:slave\n"
		} else {
			strOut += "role:master\n"
		}

		strOut += fmt.Sprintf("master_replid:%s\n", id)
		strOut += fmt.Sprintf("master_repl_offset:%s", offset)

		return Value{typ: "bulk", bulk: strOut}
	}

	return Value{typ: "error", str: "ERR error has occured with the 'config' command"}
}

func (w *Writer) set(v Value, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	var value setVal
	key := args[0].bulk

	if len(args) == 4 {
		command := strings.ToUpper(args[2].bulk)

		if command == "PX" {
			t_out, err := strconv.Atoi(args[3].bulk)

			if err != nil {
				return Value{typ: "error", str: "ERR invalid timeout value for 'PX' command"}
			}

			timeout := time.Now().Add(time.Millisecond * time.Duration(t_out))
			value.value = args[1].bulk
			value.timeout = &timeout
		}
	} else {
		value.value = args[1].bulk
		value.timeout = nil
	}

	if w.transaction {
		return w.queuer(v)
	}

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	store = append(store, struct{}{})

	//Progate writes
	w.propagate(v)

	return Value{typ: "string", str: "OK"}
}

func (w *Writer) rpush(v Value, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].bulk

	if _, ok := Lists[key]; !ok {
		Lists[key] = &LinkedList{}
	}

	values := make([]string, len(args[1:]))

	for i, val := range args[1:] {
		values[i] = val.bulk
	}

	//check if waiting for value to be added into list
	ListMu.RLock()
	if w, ok := ListWatcher[key]; ok {
		//check who has waited the longest
		slices.SortFunc(w, func(a, b ListWatchReceiver) int {
			if a.timeStarted.Before(b.timeStarted) {
				return -1
			}

			if a.timeStarted.After(b.timeStarted) {
				return 1
			}

			return 0
		})

		winner := ListWatcher[key][0]
		ListWatcher[key] = ListWatcher[key][1:]

		winner.ch <- values[0]
	}
	ListMu.RUnlock()

	result := Lists[key].Add(values...)

	return Value{typ: "integer", integer: result}
}

func (w *Writer) llen(v Value, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'llen' command"}
	}

	key := args[0].bulk

	if _, ok := Lists[key]; !ok {
		return Value{typ: "integer", integer: 0}
	}

	return Value{typ: "integer", integer: Lists[key].Length}
}

func (w *Writer) lpop(v Value, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'llop' command"}
	}

	key := args[0].bulk

	if _, ok := Lists[key]; !ok {
		return Value{typ: "bulk"}
	}

	numStr := "1"
	if len(args[0:]) > 1 {
		numStr = args[1].bulk
	}

	num, err := strconv.Atoi(numStr)
	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}

	if num == 1 {
		return Value{typ: "bulk", bulk: Lists[key].Pop()}
	}

	if num > Lists[key].Length {
		num = Lists[key].Length
	}

	returnArr := Value{typ: "array"}
	for range num {
		returnArr.array = append(returnArr.array, Value{
			typ:  "bulk",
			bulk: Lists[key].Pop(),
		})
	}

	return returnArr
}

func (w *Writer) lpush(v Value, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].bulk

	if _, ok := Lists[key]; !ok {
		Lists[key] = &LinkedList{}
	}

	values := make([]string, len(args[1:]))

	for i, val := range args[1:] {
		values[i] = val.bulk
	}

	//check if waiting for value to be added into list
	ListMu.RLock()
	if w, ok := ListWatcher[key]; ok {
		//check who has waited the longest
		slices.SortFunc(w, func(a, b ListWatchReceiver) int {
			if a.timeStarted.Before(b.timeStarted) {
				return -1
			}

			if a.timeStarted.After(b.timeStarted) {
				return 1
			}

			return 0
		})

		winner := ListWatcher[key][0]
		ListWatcher[key] = ListWatcher[key][1:]

		winner.ch <- values[0]
	}
	ListMu.RUnlock()

	result := Lists[key].RAdd(values...)

	return Value{typ: "integer", integer: result}
}

func (w *Writer) lrange(v Value, args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lrange' command"}
	}

	resultArr := Value{typ: "array"}

	key := args[0].bulk

	if _, ok := Lists[key]; !ok {
		return resultArr
	}

	start, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}

	stop, err := strconv.Atoi(args[2].bulk)
	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}

	res := Lists[key].Range(start, stop)

	if res == nil {
		return resultArr
	}

	fmt.Println("Results", res)

	for _, v := range res {
		resultArr.array = append(resultArr.array, Value{typ: "bulk", bulk: v})
	}
	return resultArr
}

func (w *Writer) lbop(v Value, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk
	fmt.Println("Hello")

	blockTime, err := strconv.ParseFloat(args[1].bulk, 64)
	if err != nil {
		return Value{typ: "error", str: "ERR invalid block time"}
	}

	if blockTime == 0 {
		blockTime = 21234324
	}

	duration := time.Duration(float64(time.Second) * blockTime)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	//Register listeners
	ListMu.Lock()
	listener := ListWatchReceiver{
		ch:          make(chan string, 1),
		timeStarted: time.Now(),
	}

	ListWatcher[key] = append(ListWatcher[key], listener)
	ListMu.Unlock()

	select {
	case res := <-readLbop(ctx, listener):
		cancel()
		returnArray := Value{typ: "array"}
		returnArray.array = append(returnArray.array, Value{typ: "bulk", bulk: key})
		returnArray.array = append(returnArray.array, Value{typ: "bulk", bulk: res})
		return returnArray
	case <-ctx.Done():
		return Value{typ: "null"}
	}
}

func readLbop(ctx context.Context, l ListWatchReceiver) chan string {
	out := make(chan string)
	go func(out chan string) {
		select {
		case res := <-l.ch:
			out <- res
			return
		case <-ctx.Done():
			close(out)
			return
		}
	}(out)

	return out
}

func (w *Writer) zadd(cmd Value, args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'zadd' command"}
	}

	key := args[0].bulk

	score, err := strconv.ParseFloat(args[1].bulk, 64)
	if err != nil {
		return Value{typ: "error", str: "ERR invalid block time"}
	}

	value := args[2].bulk

	var out int
	SortedMu.Lock()
	SortedSet[key] = NewSkipListSortedSet()
	out = SortedSet[key].Add(ListValue{
		score: score,
		name:  value,
	})
	SortedMu.Unlock()

	return Value{typ: "integer", integer: out}
}

func (w *Writer) get(cmd Value, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	if w.transaction {
		return w.queuer(cmd)
	}

	key := args[0].bulk

	SETsMu.RLock()
	val, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	if val.timeout != nil && val.timeout.Before(time.Now()) {
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()

		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val.value}
}

func (w *Writer) typeIdent(v Value, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'type'  command"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	key := args[0].bulk
	if _, ok := SETs[key]; ok {
		return Value{typ: "string", str: "string"}
	} else if _, ok := stream[key]; ok {
		return Value{typ: "string", str: "stream"}
	}

	return Value{typ: "string", str: "none"}
}

func (w *Writer) ping(v Value, args []Value) Value {
	if w.transaction {
		return w.queuer(v)
	}

	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

func (w *Writer) psync(v Value, args []Value) error {
	if len(args) != 2 {
		w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'info'  command"})
	}

	// if w.transaction {
	// 	return w.Write(w.queuer(v))
	// }

	id := ConfigMap["masterID"]
	offset := ConfigMap["masterOffset"]

	strOut := fmt.Sprintf("FULLRESYNC %s %s", id, offset)

	err := w.Write(Value{typ: "string", str: strOut})
	if err != nil {
		return err
	}

	err = w.Write(fullsync())
	if err != nil {
		return err
	}

	connMu.Lock()
	connections = append(connections, w.writer)
	connMu.Unlock()
	return nil
}

func (w *Writer) replconf(v Value, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'replconf' command"}
	}

	command := strings.ToUpper(args[0].bulk)

	switch command {
	case "GETACK":

		return Value{typ: "array", array: []Value{
			{typ: "bulk", bulk: "REPLCONF"},
			{typ: "bulk", bulk: "ACK"},
			{typ: "bulk", bulk: strconv.Itoa(offset)},
		}}

	case "ACK":
		chanChan <- true
		fmt.Println("hello angel")
		return Value{}
	default:
		return Value{typ: "string", str: "OK"}
	}
}

func (w *Writer) wait(v Value, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'wait' command"}
	}

	if len(store) == 0 {
		return Value{typ: "integer", integer: len(connections)}
	}

	acks := writeGetAck()
	err := w.propagate(acks)
	if err != nil {
		return Value{typ: "error", str: "ERR " + err.Error()}
	}

	desired, _ := strconv.Atoi(args[0].bulk)
	t, _ := strconv.Atoi(args[1].bulk)

	timer := time.After(time.Duration(t) * time.Millisecond)
	var ackBoi int

loop:
	for {
		select {
		case <-chanChan:
			ackBoi++
			if ackBoi == desired {
				break loop
			}
			fmt.Println("This si", ackBoi)
		case <-timer:
			break loop
		}
	}

	return Value{typ: "integer", integer: ackBoi}
}

func (w *Writer) keys(v Value, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'key' command"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	SETsMu.RLock()
	defer SETsMu.RUnlock()

	arrVal := Value{typ: "array"}

	for k, _ := range SETs {
		arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: k})
	}

	return arrVal
}

func (w *Writer) config(v Value, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'config' command"}
	}

	command := strings.ToUpper(args[0].bulk)

	switch command {
	case "GET":
		if len(args) == 1 {
			return w.configGetAll(v)
		}
		return w.configGet(v, args[1:])
	case "SET":
		return w.configSet(v, args[1:])
	}

	return Value{typ: "error", str: "ERR error has occured with the 'config' command"}
}

func (w *Writer) configGet(v Value, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'config get' command"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	fmt.Printf("args: %#v\n", args)
	k := args[0].bulk
	val, ok := ConfigMap[k]

	if !ok {
		return Value{typ: "null"}
	}

	arrVal := Value{typ: "array"}
	arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: k})
	arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: val})
	fmt.Printf("arrVal: %#v\n", arrVal)
	return arrVal
}

func (w *Writer) configGetAll(v Value) Value {
	if w.transaction {
		return w.queuer(v)
	}
	result := Value{typ: "array"}

	for k, v := range ConfigMap {
		result.array = append(result.array, Value{typ: "bulk", bulk: k})
		result.array = append(result.array, Value{typ: "bulk", bulk: v})
	}

	return result
}

func (w *Writer) configSet(v Value, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	key := args[0].bulk
	value := args[1].bulk

	ConfigMap[key] = value

	return Value{typ: "string", str: "OK"}
}

func (w *Writer) echo(v Value, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'echo' command"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	return Value{typ: "bulk", bulk: args[0].bulk}
}

// TRANSACTIONS
func (w *Writer) incr(v Value, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'incr' command"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	key := args[0].bulk

	SETsMu.Lock()
	defer SETsMu.Unlock()

	var num int
	var err error
	if item, ok := SETs[key]; ok {
		num, err = strconv.Atoi(item.value)
		if err != nil {
			return Value{typ: "error", str: "ERR value is not an integer or out of range"}
		}
		num++

	} else {
		num = 1
	}

	SETs[key] = setVal{timeout: nil, value: strconv.Itoa(num)}
	return Value{typ: "integer", integer: num}
}

func (w *Writer) multi(args []Value) Value {
	if w.transaction {
		return Value{typ: "error", str: "ERR MULTI called inside a transaction"}
	}
	w.transaction = true
	w.queue = make([]Value, 0)
	return Value{typ: "string", str: "OK"}
}

func (w *Writer) exec(args []Value) Value {

	if !w.transaction {
		return Value{typ: "error", str: "ERR EXEC without MULTI"}
	}

	w.transaction = false
	ret := Value{typ: "array"}

	fmt.Println(len(w.queue))
	for i := 0; i < len(w.queue); i++ {
		val := w.processQueue(w.queue[i])
		fmt.Println(val)
		ret.array = append(ret.array, val)
	}

	w.transaction = false
	return ret

}

func (w *Writer) discard(args []Value) Value {
	if w.transaction {
		w.transaction = false
		w.queue = make([]Value, 0)
		return Value{typ: "string", str: "OK"}
	}
	return Value{typ: "error", str: "ERR DISCARD without MULTI"}
}

//STREAMS

func (w *Writer) xAdd(v Value, args []Value) Value {
	if len(args) < 4 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'xadd' command"}
	}

	key := args[0].bulk // stream
	id := args[1].bulk  // stream id
	vals := args[2:]
	kvs := make([]MapKVs, 0)

	err := w.validate(key, &id)
	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}

	for i := 0; i < len(vals); i += 2 {
		k := vals[i].bulk
		if i+1 == len(vals) {
			return Value{typ: "error", str: "ERR wrong number of arguments for 'xadd' command"}
		}
		v := vals[i+1].bulk

		kvs = append(kvs, MapKVs{Key: k, Value: v})
	}

	if w.transaction {
		return w.queuer(v)
	}

	streamMu.Lock()
	defer streamMu.Unlock()
	if _, ok := stream[key]; !ok {
		stream[key] = iradix.New[[]MapKVs]()
	}
	stream[key], _, _ = stream[key].Insert([]byte(id), kvs)

	//Notify all watchers of an id if inserted id is lexicographically greater
	if prefixes, ok := watchers[key]; ok {
		for k, _ := range prefixes {
			if k == "$" {
				for _, ch := range prefixes[k] {
					ch <- WatchEvent{stream: key, id: id}
				}

				//if key are lexigraphically greater
			} else if bytes.Compare([]byte(id), []byte(k)) == 1 {
				for _, ch := range prefixes[k] {
					ch <- WatchEvent{stream: key, id: id}
				}
			}
		}

		delete(watchers, key) //cleanup
	}
	return Value{typ: "bulk", bulk: id}
}

func (w *Writer) xrange(v Value, args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'xrange' command"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	ret := Value{typ: "array"}

	key := args[0].bulk
	s := args[1].bulk
	e := args[2].bulk

	if s == "-" {
		s = "0"
	}

	streamMu.RLock()
	defer streamMu.RUnlock()

	if streamTree, ok := stream[key]; !ok {
		return Value{typ: "error", str: "ERR no such key"}
	} else {
		it := streamTree.Root().Iterator()
		it.SeekLowerBound([]byte(s))

		top, _, _ := streamTree.Root().Maximum()

		for key, treeVals, ok := it.Next(); ok; key, treeVals, ok = it.Next() {
			if e == "+" {
				e = string(top)
			}

			val := Value{typ: "array", array: []Value{{typ: "bulk", bulk: string(key)}}}

			vArr := Value{typ: "array"}
			for _, av := range treeVals {
				vArr.array = append(vArr.array, Value{typ: "bulk", bulk: av.Key})
				vArr.array = append(vArr.array, Value{typ: "bulk", bulk: av.Value})
			}

			val.array = append(val.array, vArr)
			ret.array = append(ret.array, val)

			if string(key) == e {
				break
			}

		}
	}

	return ret
}

func (w *Writer) xread(v Value, args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'xread' command"}
	}

	var blockTime int
	var block bool
	//Read block
	if strings.ToUpper(args[0].bulk) == "BLOCK" {
		time, err := strconv.Atoi(args[1].bulk)
		if err != nil {
			return Value{typ: "error", str: "ERR invalid block time"}
		}
		block = true
		blockTime = time
		args = args[2:]
	}

	if len(args[1:])%2 != 0 {
		return Value{typ: "error", str: "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified"}
	}

	if w.transaction {
		return w.queuer(v)
	}

	keyLen := len(args[1:]) / 2
	keys := make([]string, keyLen)

	for i := 0; i < keyLen; i++ {
		fmt.Println("hello angel", args[1+i])
		keys[i] = args[1+i].bulk
	}

	//Todo: verify ids

	ret := Value{typ: "array"}

	// key := args[1].bulk
	ids := args[keyLen+1:]

	//Explanation the reason why locks and unlocks are peformed in this manner is because we need to free
	//up the resource so that it can be used in the `xadd` function. Note: Might not be the best way to do this
	if block {
		streamMu.Lock()
		var exists bool
		//If blocktime is not 0 but key exists proceed in any of the provided trees proceed as now
		for i, key := range keys {
			if streamTree, ok := stream[key]; ok {
				prefix := []byte(ids[i].bulk)

				if string(prefix) == "$" {
					continue
				}
				it := streamTree.Root().Iterator()
				it.SeekLowerBound(prefix)

				for tKey, _, ok := it.Next(); ok; tKey, _, ok = it.Next() {
					if bytes.Compare(tKey, prefix) == 1 {
						exists = true
						break
					}
				}
			}
		}
		streamMu.Unlock()

		//if non of the requested ids exists
		if !exists {
			streamMu.Lock()
			chans := make([]chan WatchEvent, 0)

			//set up watch
			for i, v := range keys {
				if _, ok := watchers[v]; !ok {
					watchers[v] = make(map[string][]chan WatchEvent)
				}

				keyId := ids[i].bulk
				ch := make(chan WatchEvent, 1)
				watchers[v][keyId] = append(watchers[v][keyId], ch)
				chans = append(chans, ch)

			}
			streamMu.Unlock()

			if blockTime == 0 {
				//Feels hacky but it works
				blockTime = 21234324324343243
			}

			duration := time.Duration(blockTime * int(time.Millisecond))
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()

			select {
			case res := <-mergeChans(ctx, chans):
				cancel()

				//transform into appropriate return value:
				//first get the item from the tree
				tree := stream[res.stream]
				kvArr, _ := tree.Get([]byte(res.id))

				keyVal := Value{typ: "array", array: []Value{{typ: "bulk", bulk: res.stream}}}
				outer := Value{typ: "array"}

				treeKey := Value{typ: "array", array: []Value{{typ: "bulk", bulk: res.id}}}

				vArr := Value{typ: "array"}
				for _, av := range kvArr {
					vArr.array = append(vArr.array, Value{typ: "bulk", bulk: av.Key})
					vArr.array = append(vArr.array, Value{typ: "bulk", bulk: av.Value})
				}

				treeKey.array = append(treeKey.array, vArr)
				outer.array = append(outer.array, treeKey)
				keyVal.array = append(keyVal.array, outer)
				ret.array = append(ret.array, keyVal)
				return ret
			case <-ctx.Done():
				return Value{typ: "null"}
			}
		}

	}

	streamMu.Lock()
	defer streamMu.Unlock()
	var counter int
	for i, key := range keys {
		if streamTree, ok := stream[key]; !ok {
			counter++
			continue
		} else {
			keyVal := Value{typ: "array", array: []Value{{typ: "bulk", bulk: key}}}
			outer := Value{typ: "array"}
			id := ids[i].bulk

			it := streamTree.Root().Iterator()
			it.SeekLowerBound([]byte(id))
			top, _, _ := streamTree.Root().Maximum()

			for treekey, treeVals, ok := it.Next(); ok; treekey, treeVals, ok = it.Next() {

				if string(treekey) == id {
					continue
				}

				val := Value{typ: "array", array: []Value{{typ: "bulk", bulk: string(treekey)}}}

				vArr := Value{typ: "array"}
				for _, av := range treeVals {
					vArr.array = append(vArr.array, Value{typ: "bulk", bulk: av.Key})
					vArr.array = append(vArr.array, Value{typ: "bulk", bulk: av.Value})
				}

				val.array = append(val.array, vArr)
				outer.array = append(outer.array, val)

				_ = top
				// if string(key) == string(top) {
				// 	break
				// }

			}

			keyVal.array = append(keyVal.array, outer)
			ret.array = append(ret.array, keyVal)
		}
	}

	if counter == keyLen {
		return Value{typ: "null"}
	}
	return ret

}

func (w *Writer) validate(key string, id *string) error {
	//case for automatic id
	if *id == "*" {
		miliTime := time.Now().UnixMilli()

		streamMu.RLock()
		defer streamMu.RUnlock()

		if streamTree, ok := stream[key]; ok {
			key, _, _ := streamTree.Root().Maximum()

			tSplit := strings.Split(string(key), "-")
			tl, _ := strconv.Atoi(tSplit[0])
			tr, _ := strconv.Atoi(tSplit[1])

			if miliTime < int64(tl) {
				return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			} else if miliTime == int64(tl) {
				tr++
			}

			*id = fmt.Sprintf("%d-%d", miliTime, tr)

		} else {
			*id = fmt.Sprintf("%d-%d", miliTime, 0)
		}

		return nil
	}

	split := strings.Split(*id, "-")
	left, err := strconv.Atoi(split[0])

	if err != nil {
		return fmt.Errorf("ERR The ID specified in XADD is invalid")
	}

	//Partial Id case
	if split[1] == "*" {
		streamMu.RLock()
		defer streamMu.RUnlock()
		if streamTree, ok := stream[key]; ok {
			key, _, _ := streamTree.Root().Maximum()
			tSplit := strings.Split(string(key), "-")
			fmt.Println(string(key))
			tl, _ := strconv.Atoi(tSplit[0])
			tr, _ := strconv.Atoi(tSplit[1])

			//TODO: Fix the tr not above 10 issue

			if left < tl {
				return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			} else if left > tl {
				tl = left
				tr = 0
			} else {

				tr++
			}

			*id = fmt.Sprintf("%d-%d", tl, tr)
			return nil
		} else {
			IDSplit := strings.Split(*id, "-")
			l, _ := strconv.Atoi(IDSplit[0])
			r := 0

			if l == 0 {
				r++
			}

			*id = fmt.Sprintf("%d-%d", l, r)

			return nil
		}
	}

	right, err := strconv.Atoi(split[1])
	if err != nil {
		return fmt.Errorf("ERR The ID specified in XADD is invalid")
	}

	if left == 0 && right == 0 {
		return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	//if item exists in stream
	streamMu.RLock()
	defer streamMu.RUnlock()
	if streamTree, ok := stream[key]; ok {
		key, _, _ := streamTree.Root().Maximum()
		if string(key) == *id {

			return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

	}
	//left side must not be less than top
	if streamTree, ok := stream[key]; ok {
		key, _, _ := streamTree.Root().Maximum()
		tSplit := strings.Split(string(key), "-")
		tl, _ := strconv.Atoi(tSplit[0])
		tr, _ := strconv.Atoi(tSplit[1])

		if left < tl {
			return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

		if left >= tl && right < tr {
			return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

	}

	return nil
}

// Write propagation
func (w *Writer) propagate(v Value) error {
	multi := io.MultiWriter(connections...)
	_, err := multi.Write(v.Marshal())

	return err
}

// Transaction queuer
func (w *Writer) queuer(v Value) Value {
	w.queue = append(w.queue, v)
	return Value{typ: "string", str: "QUEUED"}
}

// Process queue
// Note: Not the ideal way to do this at all
func (w *Writer) processQueue(v Value) Value {
	command := strings.ToUpper(v.array[0].bulk)
	args := v.array[1:]

	switch command {
	case "SET":
		return w.set(v, args)
	case "GET":
		return w.get(v, args)
	case "TYPE":
		return w.typeIdent(v, args)
	case "PING":
		return w.ping(v, args)
	case "INFO":
		return w.info(v, args)
	case "KEYS":
		return w.keys(v, args)
	case "CONFIG":
		return w.config(v, args)
	case "ECHO":
		return w.echo(v, args)
	//Transactions:
	case "INCR":
		return w.incr(v, args)
	//Stream:
	case "XADD":
		return w.xAdd(v, args)
	case "XRANGE":
		return w.xrange(v, args)
	case "XREAD":
		return w.xread(v, args)
	default:
		return Value{typ: "string", str: ""}
	}
}

// Helper functions
func mergeChans(ctx context.Context, chans []chan WatchEvent) chan WatchEvent {
	out := make(chan WatchEvent, len(chans))
	// var once sync.Once
	for _, ch := range chans {
		go func(c chan WatchEvent) {
			select {
			case res := <-c:
				out <- res
				return
			case <-ctx.Done():
				close(out)
				return
			}

		}(ch)
	}

	return out
}
