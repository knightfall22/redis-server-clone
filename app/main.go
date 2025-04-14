package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

var chanChan = make(chan bool, 1)

var store []struct{}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	//Configuration setup
	InitializeConfig()

	dec := NewDecoder(ConfigMap["fullpath"])

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
	if ConfigMap.IsSlave() {
		conn, err := net.Dial("tcp", ConfigMap["replicaOf"])
		if err != nil {
			fmt.Println("Failed to bind to port ", ConfigMap["replicaOf"])
			conn.Close()
			os.Exit(1)
		}
		fmt.Println("Connected on to master")

		writer := NewWriter(conn)
		writer.Write(ping2())

		//Todo: There has to be a better way to do this
		time.Sleep(time.Second * 1)
		writer.Write(replconfLWriter())

		time.Sleep(time.Second * 1)
		writer.Write(replconfCWriter())

		time.Sleep(time.Second * 1)
		writer.Write(psyncWrite())

		go func(conn net.Conn) {
			resp := NewResp(conn)

			for {
				fmt.Println("Hello")
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

				writer := NewWriter(conn)
				err = writer.HandleSlave(value)
				if err != nil {
					fmt.Println("Error writing to connection", err.Error())
					return
				}
			}
		}(conn)

	}

	// Uncomment this block to pass the first stage
	url := fmt.Sprintf("localhost:%s", ConfigMap["port"])
	l, err := net.Listen("tcp", url)
	if err != nil {
		fmt.Println("Failed to bind to port ", ConfigMap["port"])
		os.Exit(1)
	}

	fmt.Println("Connected on port " + ConfigMap["port"])

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

				writer := NewWriter(conn)

				err = writer.Handler(value)
				if err != nil {
					fmt.Println("Error writing to connection", err.Error())
					return
				}
				continue
			}
		}(conn)
	}
}
