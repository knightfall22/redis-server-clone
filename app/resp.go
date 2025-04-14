package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

var ERRInvalidCommand = errors.New("ERR invalid command")

const (
	STRING  = '+'
	ERROR   = '-'
	ARRAY   = '*'
	MAP     = '%'
	BULK    = '$'
	INTEGER = ':'
)

type MapKVs struct {
	Key   string
	Value string
}

type Value struct {
	typ      string
	str      string
	integer  int
	len      int
	bulk     string
	array    []Value
	contents []byte
}

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
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Handler(v Value) error {
	command := strings.ToUpper(v.array[0].bulk)
	args := v.array[1:]

	switch command {
	case "SET":
		return w.set(v, args)
	case "GET":
		return w.get(args)
	case "PSYNC":
		return w.psync(args)
	case "PING":
		return w.ping(args)
	case "REPLCONF":
		return w.replconf(args)
	case "INFO":
		return w.info(args)
	case "WAIT":
		return w.wait(args)
	case "KEYS":
		return w.keys(args)
	case "CONFIG":
		return w.config(args)
	case "ECHO":
		return w.echo(args)
	default:
		return w.Write(Value{typ: "string", str: ""})
	}
}

func (w *Writer) HandleSlave(v Value) error {
	command := strings.ToUpper(v.array[0].bulk)
	args := v.array[1:]

	fmt.Println("Father of a failure", command)

	var err error

	switch command {
	case "PING":
		return w.ping(args)
	case "SET":
		err = w.set(v, args)
	case "REPLCONF":
		err = w.replconf(args)
	default:
		fmt.Println("Failed slave")
		err = w.Write(Value{typ: "string", str: ""})
	}

	valCopy := v
	valcount := len(valCopy.Marshal())
	offsetMu.Lock()
	offset += valcount
	offsetMu.Unlock()

	return err
}

func (w *Writer) info(args []Value) error {
	if len(args) == 0 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'info'  command"})
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

		return w.Write(Value{typ: "bulk", bulk: strOut})
	}

	return w.Write(Value{typ: "error", str: "ERR error has occured with the 'config' command"})
}

func (w *Writer) set(v Value, args []Value) error {
	if len(args) < 2 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"})
	}

	var value setVal
	key := args[0].bulk

	if len(args) == 4 {
		command := strings.ToUpper(args[2].bulk)

		if command == "PX" {
			t_out, err := strconv.Atoi(args[3].bulk)

			if err != nil {
				return w.Write(Value{typ: "error", str: "ERR invalid timeout value for 'PX' command"})
			}

			timeout := time.Now().Add(time.Millisecond * time.Duration(t_out))
			value.value = args[1].bulk
			value.timeout = &timeout
		}
	} else {
		value.value = args[1].bulk
		value.timeout = nil
	}

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	if ConfigMap.IsSlave() {
		return w.Write(Value{typ: "string", str: ""})
	}
	store = append(store, struct{}{})

	//Progate writes
	w.propagate(v)

	return w.Write(Value{typ: "string", str: "OK"})
}

func (w *Writer) get(args []Value) error {
	if len(args) != 1 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"})
	}

	key := args[0].bulk

	SETsMu.RLock()
	val, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return w.Write(Value{typ: "null"})
	}

	if val.timeout != nil && val.timeout.Before(time.Now()) {
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()

		return w.Write(Value{typ: "null"})
	}

	return w.Write(Value{typ: "bulk", bulk: val.value})
}

func (w *Writer) ping(args []Value) error {
	if len(args) == 0 {
		return w.Write(Value{typ: "string", str: "PONG"})
	}

	return w.Write(Value{typ: "string", str: args[0].bulk})
}

func (w *Writer) psync(args []Value) error {
	if len(args) != 2 {
		w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'info'  command"})
	}

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

func (w *Writer) replconf(args []Value) error {
	if len(args) < 2 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'replconf' command"})
	}

	command := strings.ToUpper(args[0].bulk)

	switch command {
	case "GETACK":
		return w.Write(Value{typ: "array", array: []Value{
			{typ: "bulk", bulk: "REPLCONF"},
			{typ: "bulk", bulk: "ACK"},
			{typ: "bulk", bulk: strconv.Itoa(offset)},
		}})

	case "ACK":
		chanChan <- true
		fmt.Println("hello angel")
		return w.Write(Value{})
	default:
		return w.Write(Value{typ: "string", str: "OK"})
	}
}

func (w *Writer) wait(args []Value) error {
	if len(args) < 2 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'wait' command"})
	}

	if len(store) == 0 {
		fmt.Println("I do not think you can exist --not truly anyway")
		return w.Write(Value{typ: "integer", integer: len(connections)})
	}

	fmt.Println("Who are you to tell me what I can or can't be")

	acks := writeGetAck()
	err := w.propagate(acks)
	if err != nil {
		return w.Write(Value{typ: "error", str: "ERR " + err.Error()})
	}

	desired, _ := strconv.Atoi(args[0].bulk)
	t, _ := strconv.Atoi(args[1].bulk)

	timer := time.After(time.Duration(t) * time.Millisecond)
	var ackBoi int

	fmt.Printf("the number doth haunt my dreams: (%d) (%d)\n", desired, t)
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

	return w.Write(Value{typ: "integer", integer: ackBoi})
}

func (w *Writer) keys(args []Value) error {
	if len(args) < 1 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'key' command"})
	}

	SETsMu.RLock()
	defer SETsMu.RUnlock()

	arrVal := Value{typ: "array"}

	for k, _ := range SETs {
		arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: k})
	}

	return w.Write(arrVal)
}

func (w *Writer) config(args []Value) error {
	if len(args) == 0 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'config' command"})
	}

	command := strings.ToUpper(args[0].bulk)

	switch command {
	case "GET":
		if len(args) == 1 {
			return w.Write(w.configGetAll())
		}
		return w.Write(w.configGet(args[1:]))
	case "SET":
		return w.Write(w.configSet(args[1:]))
	}

	return w.Write(Value{typ: "error", str: "ERR error has occured with the 'config' command"})
}

func (w *Writer) configGet(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'config get' command"}
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

func (w *Writer) configGetAll() Value {
	result := Value{typ: "array"}

	for k, v := range ConfigMap {
		result.array = append(result.array, Value{typ: "bulk", bulk: k})
		result.array = append(result.array, Value{typ: "bulk", bulk: v})
	}

	return result
}

func (w *Writer) configSet(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	ConfigMap[key] = value

	return Value{typ: "string", str: "OK"}
}

func (w *Writer) echo(args []Value) error {
	if len(args) == 0 {
		return w.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'echo' command"})
	}

	return w.Write(Value{typ: "bulk", bulk: args[0].bulk})
}

func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// Write propagation
func (w *Writer) propagate(v Value) error {
	multi := io.MultiWriter(connections...)
	_, err := multi.Write(v.Marshal())

	return err
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
