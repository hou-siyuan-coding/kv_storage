package cli

import (
	"fmt"
	"net"
)

type Client struct {
	// cli http.Client
}

func (c *Client) Post(url, bodyStr string) string {
	conn, err := net.Dial("tcp", "localhost:8001")
	if err != nil {
		return "client dial error" + err.Error()
	}
	writeCount, err := conn.Write([]byte(bodyStr))
	if err != nil {
		return "client write to connection error:" + err.Error()
	}
	fmt.Println("write", writeCount, "bytes successful")
	// respBuff, _ := ioutil.ReadAll(resp.Body)
	// respStr := string(respBuff)
	// defer resp.Body.Close()
	// fmt.Println("响应体：", respStr)
	return "respStr"
}
