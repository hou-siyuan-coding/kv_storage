package parser

import (
	"time"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
	"strings"

	"kv_storage/entity"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data entity.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go Parse0(reader, ch)
	return ch
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]entity.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go Parse0(reader, ch)
	var results []entity.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (entity.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go Parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
	readingRepl       bool
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func Parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err, string(debug.Stack()))
			close(ch)
		}
	}()

	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			ch <- &Payload{Err: err}
			if ioErr { // encounter io err, stop read
				close(ch)
				return
			}
			// protocol err, reset read state
			state = readState{}
			continue
		}

		// parse line
		if !state.readingMultiLine {
			// receive new response
			if msg[0] == '*' {
				// multi bulk protocol
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &entity.EmptyMultiBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else if msg[0] == '$' { // bulk protocol
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.bulkLen == -1 { // null bulk protocol
					ch <- &Payload{
						Data: &entity.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else {
				// single line protocol
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
		} else {
			// receive following bulk protocol
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// if sending finished
			if state.finished() {
				var result entity.Reply
				if state.msgType == '*' {
					result = entity.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = entity.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

func WaitReplyWithTime(reader io.Reader) (entity.Reply, bool) {
	select {
	case payload := <-ParseSingleReply(reader):
		return payload.Data, false
	case <- time.After(time.Duration(3)*time.Second):
		return nil, true
	}
}

func ParseSingleReply(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload, 1)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err, string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		// var ioErr bool
		msg, _, err = readLine(bufReader, &state)
		if err != nil {
			ch <- &Payload{Err: err}
			return ch
		}

		// parse line
		if !state.readingMultiLine {
			// receive new response
			if msg[0] == '*' {
				// multi bulk protocol
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					return ch
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &entity.EmptyMultiBulkReply{},
					}
					return ch
				}
			} else if msg[0] == '$' { // bulk protocol
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					return ch
				}
				if state.bulkLen == -1 { // null bulk protocol
					ch <- &Payload{
						Data: &entity.NullBulkReply{},
					}
					return ch
				}
			} else {
				// single line protocol
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				return ch
			}
		} else {
			// receive following bulk protocol
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				return ch
			}
			// if sending finished
			if state.finished() {
				var result entity.Reply
				if state.msgType == '*' {
					result = entity.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = entity.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				return ch
			}
		}
	}
}

func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 { // read normal line
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // read bulk line (binary safe)
		// there is CRLF between BulkReply in normal stream
		// but there is no CRLF between RDB and following AOF
		bulkLen := state.bulkLen + 2
		if state.readingRepl {
			bulkLen -= 2
		}
		msg = make([]byte, bulkLen)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		//if len(msg) == 0 ||
		//	msg[len(msg)-2] != '\r' ||
		//	msg[len(msg)-1] != '\n' {
		//	return nil, false, errors.New("protocol error: " + string(msg))
		//}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// first line of multi bulk protocol
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	}
	return errors.New("protocol error: " + string(msg))
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen >= 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	}
	return errors.New("protocol error: " + string(msg))
}

func parseSingleLineReply(msg []byte) (entity.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result entity.Reply
	switch msg[0] {
	case '+': // status protocol
		result = entity.MakeStatusReply(str[1:])
	case '-': // err protocol
		result = entity.MakeErrReply(str[1:])
	case ':': // int protocol
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = entity.MakeIntReply(val)
	default:
		// parse as text protocol
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = entity.MakeMultiBulkReply(args)
	}
	return result, nil
}

// read the non-first lines of multi bulk protocol or bulk protocol
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if len(line) > 0 && line[0] == '$' {
		// bulk protocol
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen < 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}

func CmdBytesToString(args []byte) string {
	var cmdStr string
	for _, v := range args {
		cmdStr += string(v)
	}
	return cmdStr
}
