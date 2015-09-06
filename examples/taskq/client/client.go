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
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
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

// Client represents a client to a taskq server.
type Client struct {
	sync.Mutex
	conn  net.Conn
	req   uint64
	calls map[server.ReqID]Call
	err   error
	done  chan struct{}
}

// New creates a client to the given address.
func New(addr string) (client *Client, err error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	client = &Client{
		conn:  c,
		req:   1,
		calls: make(map[server.ReqID]Call),
		done:  make(chan struct{}),
	}
	go client.serve()
	return client, nil
}

// Call represents one asynchronous request enqued by the client. Once the
// response is received, the client will send it over this channel.
type Call chan Response

// Response represents a response to a request.
type Response struct {
	ID    server.ReqID // ID is the request ID.
	Data  interface{}  // Data is the response data returned by the server.
	Error error        // Error is the response error.
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
		var req server.ReqID
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
		case bytes.Equal(rtype, server.ProtoResEnQed):
			res, err = c.handleEnQed(req, r)
			if err != nil {
				return
			}

		case bytes.Equal(rtype, server.ProtoResDeQed):
			res, err = c.handleDeQed(req, r)
			if err != nil {
				return
			}

		case bytes.Equal(rtype, server.ProtoResAcked):
			res, err = c.handleAcked(req, r)
			if err != nil {
				return
			}

		case bytes.Equal(rtype, server.ProtoResError):
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
			ID:    req,
			Error: err,
		}
	}
	c.Unlock()
}

func (c *Client) readRequest(r *bufio.Reader) (req server.ReqID, err error) {
	reqs, err := r.ReadString(' ')
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseUint(reqs[:len(reqs)-1], 10, 64)
	return server.ReqID(id), err
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

func (c *Client) handleEnQed(req server.ReqID, r *bufio.Reader) (res Response,
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
		ID:   req,
		Data: server.TaskID(tid),
	}
	return
}

func (c *Client) handleDeQed(req server.ReqID, r *bufio.Reader) (res Response,
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
		ID: req,
		Data: server.Task{
			Queue: server.Queue(q),
			ID:    server.TaskID(tid),
			Body:  d,
		},
	}
	return
}

func (c *Client) handleAcked(req server.ReqID, r *bufio.Reader) (res Response,
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
		ID: req,
		Data: server.Task{
			Queue: server.Queue(q),
			ID:    server.TaskID(tid),
		},
	}
	return
}

func (c *Client) handleError(req server.ReqID, r *bufio.Reader) (res Response,
	err error) {

	msg, err := r.ReadString('\n')
	if err != nil {
		return
	}

	res = Response{
		ID:    req,
		Error: errors.New(msg[:len(msg)-1]),
	}
	return
}

// Close closes the client and its underlying connection. All pending calls
// will be returned with an error.
func (c *Client) Close() {
	c.conn.Close()
	<-c.done
}

func (c *Client) reqID() server.ReqID {
	return server.ReqID(atomic.AddUint64(&c.req, 1))
}

func (c *Client) addCall(req server.ReqID, call Call) error {
	c.Lock()
	if c.err != nil {
		c.Unlock()
		return c.err
	}
	c.calls[req] = call
	c.Unlock()
	return nil
}

func (c *Client) getDelCall(req server.ReqID) (call Call, err error) {
	c.Lock()
	call, ok := c.calls[req]
	if !ok {
		return call, ErrRequestNotFound
	}
	delete(c.calls, req)
	c.Unlock()
	return
}

// GoEnQ enques a task with the given body in the given queue. This method
// returns the request ID and the Call represeting the enque.
//
// If call is nil in the arguments, this method returns a new call. Otherwise
// it reuses and returns the provided call.
func (c *Client) GoEnQ(queue string, body []byte, call Call) (
	server.ReqID, Call) {

	if call == nil {
		call = make(chan Response, 1)
	}
	req := c.reqID()
	if err := c.addCall(req, call); err != nil {
		call <- Response{ID: req, Error: err}
		return req, call
	}

	rstr := strconv.FormatUint(uint64(req), 10)
	lstr := strconv.Itoa(len(body))

	line := make([]byte,
		len(rstr)+len(server.ProtoReqEnQ)+len(queue)+len(lstr)+len(body)+5)

	copy(line, rstr)
	off := len(rstr)
	line[off] = ' '
	off++

	copy(line[off:], server.ProtoReqEnQ)
	off += len(server.ProtoReqEnQ)
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
		call <- Response{ID: req, Error: err}
		return req, call
	}

	return req, call
}

// GoDeQ dequeues a task from the given queue. This method returns the request
// ID and the Call represeting the enque.
//
// If call is nil in the arguments, this method returns a new call. Otherwise
// it reuses and returns the provided call.
func (c *Client) GoDeQ(queue string, call Call) (server.ReqID, Call) {
	if call == nil {
		call = make(chan Response, 1)
	}
	req := c.reqID()
	if err := c.addCall(req, call); err != nil {
		call <- Response{ID: req, Error: err}
		return req, call
	}

	rstr := strconv.FormatUint(uint64(req), 10)

	line := make([]byte, len(rstr)+len(server.ProtoReqDeQ)+len(queue)+3)

	copy(line, rstr)
	off := len(rstr)
	line[off] = ' '
	off++

	copy(line[off:], server.ProtoReqDeQ)
	off += len(server.ProtoReqDeQ)
	line[off] = ' '
	off++

	copy(line[off:], queue)
	off += len(queue)
	line[off] = '\n'

	if _, err := c.conn.Write(line); err != nil {
		call <- Response{ID: req, Error: err}
		return req, call
	}

	return req, call
}

// GoAck acknowledges a task in the given queue. This method returns the request
// ID and the Call represeting the enque.
//
// If call is nil in the arguments, this method returns a new call. Otherwise
// it reuses and returns the provided call.
func (c *Client) GoAck(queue string, taskID server.TaskID, call Call) (
	server.ReqID, Call) {

	if call == nil {
		call = make(chan Response, 1)
	}
	req := c.reqID()
	if err := c.addCall(req, call); err != nil {
		call <- Response{ID: req, Error: err}
		return req, call
	}

	rstr := strconv.FormatUint(uint64(req), 10)
	tstr := strconv.FormatUint(uint64(taskID), 10)

	line := make([]byte, len(rstr)+len(server.ProtoReqAck)+len(queue)+len(tstr)+5)

	copy(line, rstr)
	off := len(rstr)
	line[off] = ' '
	off++

	copy(line[off:], server.ProtoReqAck)
	off += len(server.ProtoReqAck)
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
		call <- Response{ID: req, Error: err}
		return req, call
	}

	return req, call
}

// EnQ enqueues a task with the given body in the queue. It returns the
// task's id or an error.
func (c *Client) EnQ(ctx context.Context, queue string, body []byte) (
	id server.TaskID, err error) {

	_, call := c.GoEnQ(queue, body, nil)
	select {
	case res := <-call:
		if res.Error != nil {
			return 0, res.Error
		}
		return res.Data.(server.TaskID), res.Error
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// DeQ dequeues a task from the given queue. It returns the task or an error.
func (c *Client) DeQ(ctx context.Context, queue string) (task server.Task,
	err error) {

	_, call := c.GoDeQ(queue, nil)
	select {
	case res := <-call:
		if res.Error != nil {
			return server.Task{}, res.Error
		}
		return res.Data.(server.Task), nil
	case <-ctx.Done():
		return server.Task{}, ctx.Err()
	}
}

// Ack acknowledges a task in the queue.
func (c *Client) Ack(ctx context.Context, queue string, task server.TaskID) (
	err error) {
	_, call := c.GoAck(queue, task, nil)
	select {
	case res := <-call:
		return res.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}
