package main

import (
	"fmt"
	"kv_storage/datastruct"
)

func main() {
	l := datastruct.NewList()
	vs := l.Lrange(0, -2)
	fmt.Println(vs)
	fmt.Println(l.GetLength())
	l.Rpush([]string{"a", "b", "c"})
	vs = l.Lrange(0, -2)
	fmt.Println(vs)
	fmt.Println(l.GetLength())
	l.Lpush([]string{"1", "2", "3"})
	vs = l.Lrange(0, -2)
	fmt.Println(vs)
	fmt.Println(l.GetLength())

	// conn, _ := net.Dial("tcp", "localhost:8001")
	// defer conn.Close()

	// sc := bufio.NewScanner(os.Stdin)
	// for sc.Scan() {
	// 	input := sc.Text() + "\n"
	// 	if input == "quit\n" {
	// 		break
	// 	}
	// 	writeCount, err := conn.Write([]byte(input))
	// 	if err != nil {
	// 		fmt.Println(err.Error())
	// 	}
	// 	fmt.Println("client write successful", writeCount, "bytes")

	// 	reader := bufio.NewReader(conn)
	// 	resp, err := reader.ReadBytes('\n')
	// 	if err != nil {
	// 		fmt.Println(err.Error())
	// 	}
	// 	fmt.Println("resp body:", string(resp))
	// }
}
