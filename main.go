package main

import (
	"bufio"
	"fmt"
	"kv_storage/cli"
	"os"
)

func main() {
	client := cli.Client{}
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		respStr := client.Post("http://localhost:8001", sc.Text())
		fmt.Println("响应体：", respStr)
	}
}
