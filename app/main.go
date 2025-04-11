package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	connections []io.Writer
	connMu      sync.Mutex
)

var (
	offset   = 0
	offsetMu sync.Mutex
)

var chanChan = make(chan bool, 1)

var store []struct{}

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
		if err != nil && err != io.EOF {
			fmt.Println("Err: error has occurred: ", err)
			os.Exit(1)
		}
	}

	fmt.Println("SETS", SETs)

	//if slave connect to master
	if *replicaOf != "" {
		conn, err := net.Dial("tcp", *replicaOf)
		if err != nil {
			fmt.Println("Failed to bind to port ", *replicaOf)
			conn.Close()
			os.Exit(1)
		}
		fmt.Println("Connected on to master")

		writer := NewWriter(conn)
		writer.Write(ping2())

		//Todo: There has to be a better way to do this
		time.Sleep(time.Second * 1)
		writer.Write(replconfLWriter(*port))

		time.Sleep(time.Second * 1)
		writer.Write(replconfCWriter())

		time.Sleep(time.Second * 1)
		writer.Write(psyncWrite())

		go func(conn net.Conn) {
			resp := NewResp(conn)

			for {
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

				handle, ok := Handlers[command]
				if !ok {
					fmt.Println("Invalid command: ", command)
					writer.Write(Value{typ: "string", str: ""})
					continue
				}

				val := handle(args)
				writer := NewWriter(conn)

				if command == "REPLCONF" && strings.ToUpper(args[0].bulk) == "GETACK" {
					writer.Write(val)
				}

				valCopy := value
				valcount := len(valCopy.Marshal())
				offsetMu.Lock()
				offset += valcount
				offsetMu.Unlock()

				// writer.Write(writeAck())
			}
		}(conn)

	}

	// Uncomment this block to pass the first stage
	url := fmt.Sprintf("localhost:%s", *port)
	l, err := net.Listen("tcp", url)
	if err != nil {
		fmt.Println("Failed to bind to port ", *port)
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
			resp := NewResp(conn)
			for {

				value, err := resp.Read()

				fmt.Println(conn.RemoteAddr(), value)
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

				//Test propagation
				if command == "SET" {
					multi := io.MultiWriter(connections...)
					_, err = multi.Write(value.Marshal())

					if err != nil {
						return
					}

				}

				if command == "WAIT" && len(store) <= 0 {
					fmt.Println("See")
					acks := writeGetAck()
					multi := io.MultiWriter(connections...)
					_, err = multi.Write(acks.Marshal())

					if err != nil {
						return
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

					fmt.Println(ackBoi)
					writer.Write(Value{typ: "integer", integer: ackBoi})
					continue
				}

				handle, ok := Handlers[command]
				if !ok {
					fmt.Println("Invalid command: ", command)
					writer.Write(Value{typ: "string", str: ""})
					continue
				}

				result := handle(args)

				fmt.Println(result)
				writer.Write(result)

				//Todo: refactor probably not the best way to do this

				if command == "PSYNC" {
					writer.Write(fullsync())

					connMu.Lock()
					connections = append(connections, conn)
					connMu.Unlock()
				}

			}
		}(conn)
	}
}
