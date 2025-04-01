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
	dir := flag.String("dir", ".", "Directory containing db file")
	dbfilename := flag.String("dbfilename", "dump.rdb", "Database file")

	flag.Parse()

	//Configuration setup
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

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
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
