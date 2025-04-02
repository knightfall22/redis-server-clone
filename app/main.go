package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	//Flags to accept the configs
	dir := flag.String("dir", ".", "Directory containing db file")
	dbfilename := flag.String("dbfilename", "dump.rdb", "Database file")
	port := flag.String("port", "6379", "Port number")
	replicaOf := flag.String("replicaof", "", "set has replica of a master")

	flag.Parse()

	//Configuration setup
	ConfigMu.Lock()
	Config["dir"] = *dir
	Config["dbfilename"] = *dbfilename
	Config["port"] = *port
	if *replicaOf != "" {
		splitedStr := strings.Split(*replicaOf, " ")
		*replicaOf = strings.Join(splitedStr, ":")
	}
	Config["replicaOf"] = *replicaOf
	Config["masterID"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	Config["masterOffset"] = "0"
	ConfigMu.Unlock()

	dec := NewDecoder(Config["dir"] + "/" + Config["dbfilename"])

	//when file is found
	if dec != nil {
		err := dec.Reader()
		if err != nil {
			fmt.Println("Err: error has occurred: ", err)
			os.Exit(1)
		}
	}

	fmt.Println("SETS", SETs)

	//if slave connect to master
	if *replicaOf != "" {
		conn, err := net.Dial("tcp", *replicaOf)
		if err != nil {
			fmt.Println("Failed to bind to port 6379")
			os.Exit(1)
		}
		fmt.Println("Connected on to master")

		writer := NewWriter(conn)
		writer.Write(ping2())

		time.Sleep(time.Second * 2)
		writer.Write(replconfLWriter(*port))
		time.Sleep(time.Second * 2)
		writer.Write(replconfCWriter())
	}

	// Uncomment this block to pass the first stage
	url := fmt.Sprintf("localhost:%s", *port)
	l, err := net.Listen("tcp", url)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	fmt.Println("Connected on port " + *port)

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
