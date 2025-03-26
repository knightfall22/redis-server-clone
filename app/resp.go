package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	ARRAY   = '*'
	BULK    = '$'
	INTEGER = ':'
)

type Value struct {
	typ   string
	str   string
	bulk  string
	array []Value
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

func (v *Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}
