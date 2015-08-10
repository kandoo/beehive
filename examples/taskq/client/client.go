package client

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/examples/taskq/server"
)

var (
	// ErrInvalidResType returned when the response type sent by the server is
	// invalid.
	ErrInvalidResType = errors.New("taskq client: invalid response type")
	// ErrRequestNotFound returned when the request id is not found.
	ErrRequestNotFound = errors.New("taskq client: request not found")
	// ErrInvalidResponse returned when the server response is invalid.
	ErrInvalidResponse = errors.New("taskq client: invalid response")
)

type Client struct {
	sync.Mutex
	conn  net.Conn
	req   uint64
	calls map[server.Request]Call
	err   error
	done  chan struct{}
}

func New(addr string) (client *Client, err error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	client = &Client{
		conn:  c,
		req:   1,
		calls: make(map[server.Request]Call),
		done:  make(chan struct{}),
	}
	go client.serve()
	return client, nil
}

type Call chan Response

type Response struct {
	Request server.Request
	Data    interface{}
	Error   error
}

func (c *Client) serve() {
	var err error

	defer func() {
		c.conn.Close()
		c.broadcastError(err)
		if err != nil && err != io.EOF {
			if _, ok := err.(net.Error); !ok {
				glog.Errorf("error in server response: %v", err)
			}
		}
		close(c.done)
	}()

	r := bufio.NewReader(c.conn)
	for {
		var req server.Request
		if req, err = c.readRequest(r); err != nil {
			return
		}

		var call Call
		if call, err = c.getDelCall(req); err != nil {
			return
		}

		var rtype []byte
		if rtype, err = c.readResponseType(r); err != nil {
			return
		}

		var res Response
		switch {
		case bytes.Equal(rtype, server.ProtoRepEnQed):
			res, err = c.handleEnQed(req, r)
			if err != nil {
				return
			}

		case bytes.Equal(rtype, server.ProtoRepDeQed):
			res, err = c.handleDeQed(req, r)
			if err != nil {
				return
			}

		case bytes.Equal(rtype, server.ProtoRepAcked):
			res, err = c.handleAcked(req, r)
			if err != nil {
				return
			}

		case bytes.Equal(rtype, server.ProtoRepError):
			res, err = c.handleError(req, r)
			if err != nil {
				return
			}

		default:
			err = ErrInvalidResType
		}

		if err != nil {
			return
		}

		call <- res
	}
}

func (c *Client) broadcastError(err error) {
	c.Lock()
	c.err = err
	for req, call := range c.calls {
		call <- Response{
			Request: req,
			Error:   err,
		}
	}
	c.Unlock()
}

func (c *Client) readRequest(r *bufio.Reader) (req server.Request, err error) {
	reqs, err := r.ReadString(' ')
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseUint(reqs[:len(reqs)-1], 10, 64)
	return server.Request(id), err
}

func (c *Client) readResponseType(r *bufio.Reader) (res []byte, err error) {
	res = make([]byte, server.ResLen)
	if n, err := io.ReadAtLeast(r, res, server.ResLen); n != server.ResLen {
		if err == io.EOF {
			return nil, err
		}
		return nil, ErrInvalidResType
	}

	if c, err := r.ReadByte(); err != nil && c != ' ' {
		return nil, ErrInvalidResType
	}

	return res, nil
}

func (c *Client) handleEnQed(req server.Request, r *bufio.Reader) (res Response,
	err error) {

	q, err := r.ReadString(' ')
	if err != nil {
		return
	}
	q = q[:len(q)-1]

	s, err := r.ReadString('\n')
	if err != nil {
		return
	}
	s = s[:len(s)-1]

	tid, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return
	}

	res = Response{
		Request: req,
		Data:    server.TaskID(tid),
	}
	return
}

func (c *Client) handleDeQed(req server.Request, r *bufio.Reader) (res Response,
	err error) {

	q, err := r.ReadString(' ')
	if err != nil {
		return
	}
	q = q[:len(q)-1]

	s, err := r.ReadString(' ')
	if err != nil {
		return
	}

	tid, err := strconv.Atoi(string(s[:len(s)-1]))
	if err != nil {
		return
	}

	s, err = r.ReadString(' ')
	if err != nil {
		return
	}

	l, err := strconv.Atoi(string(s[:len(s)-1]))
	if err != nil {
		return
	}

	var d []byte
	d = make([]byte, l)
	n, err := io.ReadAtLeast(r, d, l)
	if err != nil {
		return
	}
	if n != l {
		err = ErrInvalidResponse
		return
	}

	b, err := r.ReadByte()
	if err != nil {
		return
	}
	if b != '\n' {
		err = ErrInvalidResponse
		return
	}

	res = Response{
		Request: req,
		Data: server.Task{
			Queue: server.Queue(q),
			ID:    server.TaskID(tid),
			Body:  d,
		},
	}
	return
}

func (c *Client) handleAcked(req server.Request, r *bufio.Reader) (res Response,
	err error) {

	q, err := r.ReadString(' ')
	if err != nil {
		return
	}
	q = q[:len(q)-1]

	s, err := r.ReadString(' ')
	if err != nil {
		return
	}

	tid, err := strconv.Atoi(string(s[:len(s)-1]))
	if err != nil {
		return
	}

	res = Response{
		Request: req,
		Data: server.Task{
			Queue: server.Queue(q),
			ID:    server.TaskID(tid),
		},
	}
	return
}

func (c *Client) handleError(req server.Request, r *bufio.Reader) (res Response,
	err error) {

	msg, err := r.ReadString('\n')
	if err != nil {
		return
	}

	res = Response{
		Request: req,
		Error:   errors.New(msg[:len(msg)-1]),
	}
	return
}

func (c *Client) Close() {
	c.conn.Close()
	<-c.done
}

func (c *Client) reqID() server.Request {
	return server.Request(atomic.AddUint64(&c.req, 1))
}

func (c *Client) addCall(req server.Request, call Call) error {
	c.Lock()
	if c.err != nil {
		c.Unlock()
		return c.err
	}
	c.calls[req] = call
	c.Unlock()
	return nil
}

func (c *Client) getDelCall(req server.Request) (call Call, err error) {
	c.Lock()
	call, ok := c.calls[req]
	if !ok {
		return call, ErrRequestNotFound
	}
	delete(c.calls, req)
	c.Unlock()
	return
}

func (c *Client) DoEnQ(queue string, body []byte, call Call) (
	server.Request, Call) {

	if call == nil {
		call = make(chan Response, 1)
	}
	req := c.reqID()
	if err := c.addCall(req, call); err != nil {
		call <- Response{Request: req, Error: err}
		return req, call
	}

	rstr := strconv.FormatUint(uint64(req), 10)
	lstr := strconv.Itoa(len(body))

	line := make([]byte,
		len(rstr)+len(server.ProtoCmdEnQ)+len(queue)+len(lstr)+len(body)+5)

	copy(line, rstr)
	off := len(rstr)
	line[off] = ' '
	off++

	copy(line[off:], server.ProtoCmdEnQ)
	off += len(server.ProtoCmdEnQ)
	line[off] = ' '
	off++

	copy(line[off:], queue)
	off += len(queue)
	line[off] = ' '
	off++

	copy(line[off:], lstr)
	off += len(lstr)
	line[off] = ' '
	off++

	copy(line[off:], body)
	off += len(body)
	line[off] = '\n'

	if _, err := c.conn.Write(line); err != nil {
		call <- Response{Request: req, Error: err}
		return req, call
	}

	return req, call
}

func (c *Client) DoDeQ(queue string, call Call) (server.Request, Call) {
	if call == nil {
		call = make(chan Response, 1)
	}
	req := c.reqID()
	if err := c.addCall(req, call); err != nil {
		call <- Response{Request: req, Error: err}
		return req, call
	}

	rstr := strconv.FormatUint(uint64(req), 10)

	line := make([]byte, len(rstr)+len(server.ProtoCmdDeQ)+len(queue)+3)

	copy(line, rstr)
	off := len(rstr)
	line[off] = ' '
	off++

	copy(line[off:], server.ProtoCmdDeQ)
	off += len(server.ProtoCmdDeQ)
	line[off] = ' '
	off++

	copy(line[off:], queue)
	off += len(queue)
	line[off] = '\n'

	if _, err := c.conn.Write(line); err != nil {
		call <- Response{Request: req, Error: err}
		return req, call
	}

	return req, call
}

func (c *Client) DoAck(queue string, taskID server.TaskID, call Call) (
	server.Request, Call) {

	if call == nil {
		call = make(chan Response, 1)
	}
	req := c.reqID()
	if err := c.addCall(req, call); err != nil {
		call <- Response{Request: req, Error: err}
		return req, call
	}

	rstr := strconv.FormatUint(uint64(req), 10)
	tstr := strconv.FormatUint(uint64(taskID), 10)

	line := make([]byte, len(rstr)+len(server.ProtoCmdAck)+len(queue)+len(tstr)+5)

	copy(line, rstr)
	off := len(rstr)
	line[off] = ' '
	off++

	copy(line[off:], server.ProtoCmdAck)
	off += len(server.ProtoCmdAck)
	line[off] = ' '
	off++

	copy(line[off:], queue)
	off += len(queue)
	line[off] = ' '
	off++

	copy(line[off:], tstr)
	off += len(tstr)
	line[off] = '\n'

	if _, err := c.conn.Write(line); err != nil {
		call <- Response{Request: req, Error: err}
		return req, call
	}

	return req, call
}

func (c *Client) EnQ(queue string, body []byte) (id server.TaskID, err error) {
	_, call := c.DoEnQ(queue, body, nil)
	res := <-call
	if res.Error != nil {
		return 0, res.Error
	}
	return res.Data.(server.TaskID), res.Error
}

func (c *Client) Ack(queue string, task server.TaskID) error {
	_, call := c.DoAck(queue, task, nil)
	res := <-call
	return res.Error
}

func (c *Client) DeQ(queue string) (task server.Task, err error) {
	_, call := c.DoDeQ(queue, nil)
	res := <-call
	if res.Error != nil {
		return server.Task{}, res.Error
	}
	return res.Data.(server.Task), nil
}
