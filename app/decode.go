package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	RESIZEDBCODE byte = 251
	EXPIREMS     byte = 252 /* Expire time in milliseconds. */
	EXPIRES      byte = 253 /* Old expire time in seconds. */
)

const (
	SIXBIT          byte = 0
	FOURTEENBIT     byte = 1
	ENCODEDVALUEBIT byte = 3

	//Integer encoding
	EIGHTBITINT     byte = 0
	SIXTEENBITINT   byte = 1
	THIRTYTWOBITINT byte = 2
	SIXTYFOURBITINT byte = 3
)

// types
const (
	STRINGTYP byte = 0
)

type Decoder struct {
	rd *bufio.Reader
	b  byte
}

func NewDecoder(dir string) *Decoder {
	//Open RDB file
	f, err := os.Open(dir)
	if err != nil {
		fmt.Println("err: error has occurred opening file: ", err)
		return nil //when file is not found. Means db is empty
	}

	return &Decoder{
		rd: bufio.NewReader(f),
	}
}

// fills the approapraite data structure with data from RDB
func (r *Decoder) Reader() error {
	//Read up to database ignoring all other sections of the file
	_, err := r.rd.ReadBytes(RESIZEDBCODE)
	if err != nil {
		fmt.Println("err: error has occurred opening file: ", err)
		return err
	}

	//Get length of hash table and length of expiration table
	hlen, elen, err := r.getTablesLength()
	if err != nil {
		fmt.Println("err: error has occurred from gettableLenght: ", err)
		return err
	}

	fmt.Println("hash length", hlen)
	fmt.Println(elen)
	fmt.Println("current byte", r.b)

	//Build key value pairs for hash table
	r.processHash(hlen)
	return nil
}

// Will need to change in order to handle more than just simple KVs
func (r *Decoder) processHash(hlen int64) error {
	for i := int64(0); i < hlen; i++ {
		err := r.process(nil)
		if err != nil {
			fmt.Println("err: error has occurred from processHash: ", err)
			return err
		}
	}

	return nil
}

func (r *Decoder) process(val *setVal) error {
	switch r.b {
	case STRINGTYP:
		fmt.Println("STRINGTYP")
		err := r.advance()
		fmt.Println("string advance", r.b)
		if err != nil {
			fmt.Println("err: error has occurred from process: ", err)
			return err
		}
		key, err := r.readLine()
		if err != nil {
			fmt.Println("err: error has occurred from process: ", err)
			return err
		}
		value, err := r.readLine()
		if err != nil {
			fmt.Println("err: error has occurred from process: ", err)
			return err
		}

		SETsMu.Lock()
		//This might not be the best way to do this
		if val != nil {
			//If val is not nil then our timeout property exists
			val.value = value
			SETs[key] = *val
		} else {
			SETs[key] = setVal{value, nil}
		}

		fmt.Println("SETs: ", SETs)
		SETsMu.Unlock()
		return nil

	case EXPIREMS:
		fmt.Println("EXPIREMS")
		timeout, err := r.readLine()
		if err != nil {
			fmt.Println("err: error has occurred from process: ", err)
			return err
		}
		// //Convert to int
		t_out, err := strconv.Atoi(timeout)
		if err != nil {
			fmt.Println("err: error has occurred from process: ", err)
			return err
		}

		time := time.Now().Add(time.Millisecond * time.Duration(t_out))
		val = &setVal{timeout: &time}

		err = r.process(val)

		return err
	}

	return nil
}

func (r *Decoder) readLine() (string, error) {
	//current byte would by of encoded length
	//0Xc0 - 11000000
	fmt.Printf("r.b: 0x%X (%08b)\n", r.b, r.b)
	// typ := (b & 0xc0) >> 6
	typ := (r.b & 0xc0) >> 6
	switch typ {
	case SIXBIT:
		length := uint64(r.b & 0x3f) //0x03f - 00111111
		val := make([]byte, length)
		err := r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in readLine: ", err)
			return "", err
		}

		return string(val), nil
	//Todo: 14 bit case is not being meet need to fix that
	case FOURTEENBIT:
		b := r.b
		err := r.advance()
		if err != nil {
			fmt.Println("err: error has occurred from processHash: ", err)
			return "", err
		}
		length := (uint64(b&0x3f) << 8) | uint64(r.b)
		val := make([]byte, length)
		err = r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in readLine: ", err)
			return "", err
		}

		return string(val), nil

	case ENCODEDVALUEBIT:
		_typ := byte(r.b&0x3f) >> 4 //Shift right 4 bits
		return r.handleSpecialCase(_typ)

	}

	return "", nil
}

func (r *Decoder) handleSpecialCase(b byte) (string, error) {
	switch b {
	case EIGHTBITINT:
		val := make([]byte, 1)
		err := r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in handleSpecialCase: ", err)
			return "", err
		}

		return bytesToString(val), nil

	case SIXTEENBITINT:
		val := make([]byte, 2)
		err := r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in handleSpecialCase: ", err)
			return "", err
		}

		return bytesToString(val), nil
	case THIRTYTWOBITINT:
		val := make([]byte, 4)
		err := r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in handleSpecialCase: ", err)
			return "", err
		}

		return bytesToString(val), nil
	case SIXTYFOURBITINT:
		fmt.Println("Hello")
		val := make([]byte, 8)
		err := r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in handleSpecialCase: ", err)
			return "", err
		}

		return bytesToString(val), nil

	}

	return "", nil
}

func (r *Decoder) getTablesLength() (int64, int64, error) {
	lengths := make([]byte, 2)
	_, err := r.rd.Read(lengths)
	if err != nil {
		fmt.Println("Err: error has occurred: ", err)
		return 0, 0, err
	}

	err = r.advance()
	if err != nil {
		fmt.Println("Err: error has occurred: ", err)
		return 0, 0, err
	}

	fmt.Println(lengths)

	return int64(lengths[0]), int64(lengths[1]), nil
}

func (r *Decoder) advance() error {
	b, err := r.rd.ReadByte()
	if err != nil {
		fmt.Println("Err: error has occurred advancing: ", err)
		return err
	}

	r.b = b
	return nil
}

func (r *Decoder) read(p *[]byte) error {
	_, err := r.rd.Read(*p)
	if err != nil {
		fmt.Println("Err: error has occurred in decoder read: ", err)
		return err
	}

	err = r.advance()
	if err != nil {
		fmt.Println("Err: error has occurred in decoder read: ", err)
		return err
	}

	return err
}

func bytesToString(bytes []byte) string {
	result := ""
	for _, b := range bytes {
		result += strconv.Itoa(int(b))
	}
	return result
}
