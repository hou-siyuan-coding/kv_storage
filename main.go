package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:8001")
	defer conn.Close()

	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		input := sc.Text() + "\n"
		if input == "quit\n" {
			break
		}
		writeCount, err := conn.Write([]byte(input))
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("client write successful", writeCount, "bytes")

		reader := bufio.NewReader(conn)
		resp, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("resp body:", string(resp))
	}
}
