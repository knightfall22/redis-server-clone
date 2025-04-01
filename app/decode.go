package main

import (
	"bufio"
	"fmt"
	"os"
)

const (
	RESIZEDBCODE = 251
)

const (
	SIXBIT      = 0
	FOURTEENBIT = 1
)

// types
const (
	STRINGTYP = 0
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
		os.Exit(1)
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
	fmt.Println("hash length", r.b)

	//Build key value pairs for hash table
	r.processHash(hlen)
	return nil
}

// Will need to change in order to handle more than just simple KVs
func (r *Decoder) processHash(hlen int64) error {
	for i := int64(0); i < hlen; i++ {
		switch r.b {
		case STRINGTYP:
			err := r.advance()
			fmt.Println("our advance", r.b)
			if err != nil {
				fmt.Println("err: error has occurred from processHash: ", err)
				return err
			}
			key, err := r.readLine()
			if err != nil {
				fmt.Println("err: error has occurred from processHash: ", err)
				return err
			}
			value, err := r.readLine()
			if err != nil {
				fmt.Println("err: error has occurred from processHash: ", err)
				return err
			}

			SETsMu.Lock()
			SETs[key] = setVal{value, nil}
			SETsMu.Unlock()
		}
	}

	return nil
}

func (r *Decoder) readLine() (string, error) {
	//current byte would by of encoded length
	//0Xc0 - 11000000
	fmt.Printf("r.b: 0x%X (%08b)\n", r.b, r.b)
	// typ := (b & 0xc0) >> 6
	typ := (r.b & 0xc0) >> 6
	fmt.Println("typ", typ)
	switch typ {
	case SIXBIT:
		length := uint64(r.b & 0x3f) //0x03f - 00111111
		fmt.Println(length)
		val := make([]byte, length)
		err := r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in readLine: ", err)
			return "", err
		}

		return string(val), nil
	//Todo: 14 bit case is not being meet need to fix that
	case FOURTEENBIT:
		fmt.Println("Hey")
		b := r.b
		err := r.advance()
		if err != nil {
			fmt.Println("err: error has occurred from processHash: ", err)
			return "", err
		}
		length := (uint64(b&0x3f) << 8) | uint64(r.b)
		fmt.Println(length)
		val := make([]byte, length)
		err = r.read(&val)
		if err != nil {
			fmt.Println("Err: error has occurred in readLine: ", err)
			return "", err
		}

		return string(val), nil
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
