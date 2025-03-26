package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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

		defer conn.Close()

		go func() {
			buf := make([]byte, 1024)
			_, err = conn.Read(buf)
			if err != nil && err != io.EOF {
				fmt.Println("Error reading from connection", err.Error())
				os.Exit(1)
			}

			conn.Write([]byte("+PONG\r\n"))
		}()
	}
}
