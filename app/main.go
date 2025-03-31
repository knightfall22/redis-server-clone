package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	//Flags to accept the configs
	dir := flag.String("dir", "", "Directory containing db file")
	dbfilename := flag.String("dbfilename", "dump.rdb", "Database file")

	flag.Parse()

	ConfigMu.Lock()
	Config["dir"] = *dir
	Config["dbfilename"] = *dbfilename
	ConfigMu.Unlock()

	dec := NewDecoder(*dir + "/" + *dbfilename)
	err := dec.Reader()
	if err != nil {
		fmt.Println("Err: error has occurred: ", err)
		os.Exit(1)
	}
	//TODO: Delete
	//Open file
	// file, err := os.Open("dump.rdb")
	// if err != nil {
	// 	fmt.Println("Err: error has occurred: ", err)
	// 	os.Exit(1)
	// }

	// reader := bufio.NewReader(file)
	// _, _ = reader.ReadBytes(251)

	// var buf bytes.Buffer
	// if _, err := io.Copy(&buf, reader); err != nil {

	// }

	// fmt.Println(buf.Bytes())

	// //read length of hash table
	// lengths := make([]byte, 2)
	// _, err = reader.Read(lengths)
	// if err != nil {
	// 	fmt.Println("Err: error has occurred: ", err)
	// 	os.Exit(1)
	// }

	// //hash table length
	// hl := int(lengths[0])

	// //expire table length
	// _ = int(lengths[1])

	// //fetch encoded type
	// b, err := reader.ReadByte()
	// if err != nil {
	// 	fmt.Println("Err: error has occurred: ", err)
	// 	os.Exit(1)
	// }

	// fmt.Println(hl)

	// switch b {
	// case 0:
	// 	b, err = reader.ReadByte()
	// 	if err != nil {
	// 		fmt.Println("Err: error has occurred: ", err)
	// 		os.Exit(1)
	// 	}

	// 	for i := 0; i < hl; i++ {
	// 		typ := (b & 0xc0) >> 6

	// 		switch typ {
	// 		case 0:
	// 			sl := uint64(b & 0x3f)
	// 			fmt.Println(sl)
	// 			key := make([]byte, sl)
	// 			_, err = reader.Read(key)

	// 			b, err = reader.ReadByte()
	// 			if err != nil {
	// 				fmt.Println("Err: error has occurred: ", err)
	// 				os.Exit(1)
	// 			}
	// 			fmt.Println(b)
	// 		case 1:
	// 			fmt.Println("hello")
	// 			bb, err := reader.ReadByte()
	// 			if err != nil {
	// 				fmt.Println("Err: error has occurred: ", err)
	// 				os.Exit(1)
	// 			}

	// 			sl := (uint64((b & 0x3f)) << 8) | uint64(bb)
	// 			fmt.Println(sl)
	// 			key := make([]byte, sl)
	// 			_, err = reader.Read(key)

	// 			b, err = reader.ReadByte()
	// 			if err != nil {
	// 				fmt.Println("Err: error has occurred: ", err)
	// 				os.Exit(1)
	// 			}
	// 			fmt.Println(string(key))
	// 		}
	// 	}

	// }

	//Convert to integer
	// i64 := int64(b)
	// if err != nil {
	// 	fmt.Println("Err: error has occurred: ", err)
	// 	os.Exit(1)
	// }

	// fmt.Println(i64)

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go func(conn net.Conn) {
			defer conn.Close()
			for {

				resp := NewResp(conn)
				value, err := resp.Read()
				if err != nil {
					fmt.Println("Error reading from connection", err.Error())
					return
				}

				if value.typ != "array" {
					fmt.Println("Invalid request, expected array")
					continue
				}

				if len(value.array) == 0 {
					fmt.Println("Invalid request, expected array length > 0")
					continue
				}

				command := strings.ToUpper(value.array[0].bulk)
				args := value.array[1:]

				writer := NewWriter(conn)

				handle, ok := Handlers[command]
				if !ok {
					fmt.Println("Invalid command: ", command)
					writer.Write(Value{typ: "string", str: ""})
					continue
				}

				result := handle(args)
				writer.Write(result)
			}
		}(conn)
	}
}
