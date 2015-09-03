package server

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/soheilhy/args"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/kandoo/beehive/bucket"
)

const (
	// queues is a logical dictionary name used in map functions.
	queues = "q"
	// active is the name of the active dictionary.
	active = "active"
	// dequed is the name of the dequed dictionary.
	dequed = "dequed"
	// ooo is the name of the out of order dictionary.
	ooo = "ooo"
	// httpTimeout represents the timeout for sync request from http handlers.
	httpTimeout = 30 * time.Second
)

var (
	// ErrEmptyQueue is returned when dequeing an empty queue.
	ErrEmptyQueue = errors.New("taskq: no task in queue")
	// ErrNoTask is returned when acking or accessing a task that does not exist.
	ErrNoSuchTask = errors.New("taskq: no such task")
	// ErrInvalidCmd is returned by the protocol handler whenever an invalid
	// command is sent to the server.
	ErrInvalidCmd = errors.New("taskq: invalid command")
	// ErrNotEnoughData is returned by the protocol handler whenever the data sent
	// by the client is less than the length specificed.
	ErrNotEnoughData = errors.New("taskq: not enough data")
	// ErrNoNewLine is returned by the protocol handler whenever the request sent
	// by the client does not end in a new line.
	ErrNoNewLine = errors.New("taskq: new line expected")
)

// Queue represents a named queue.
type Queue string

// TaskID represents the ID of a task.
type TaskID uint64

func (id TaskID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

// Task represents a task in a queue.
type Task struct {
	ID    TaskID `json:"id"`    // Task's globally unique ID assigned by taskq.
	Queue Queue  `json:"queue"` // Task's queue.
	Body  []byte `json:"body"`  // Task's client data.
}

// dqTask is a dequed task that has a timestamp in addition to the task.
type dqTask struct {
	Task
	DequeTime time.Time // When the task is dequed.
}

// Request contains a client assigned request ID.
type Request uint64

func (r Request) String() string {
	return strconv.FormatUint(uint64(r), 10)
}

// Enqueue enqueus a task.
type Enque struct {
	Request Request // The client request.
	Queue   Queue   // The queue to enque the task.
	Body    []byte  // The body of the task to be enqued.
}

// Deque represents a message emitted to dequeue a task from a queue.
type Deque struct {
	Request Request // The client request.
	Queue   Queue   // The queue to dequeue a task from.
}

// Ack represents a message emitted to acknowledge a previously dequeued task.
type Ack struct {
	Request Request // The client request.
	Queue   Queue   // The queue.
	TaskID  TaskID  // The task ID.
}

// Enqued represents a message emitted as a reply to a successful Enque request.
type Enqued struct {
	Request Request // The client request.
	Queue   Queue   // The queue that the task is enqueued in.
	TaskID  TaskID  // The assigned ID of the enqueued task.
}

func (e Enqued) writeTo(w *bufio.Writer) (err error) {
	if _, err = w.WriteString(e.Request.String()); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.Write(ProtoRepEnQed); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.Write([]byte(e.Queue)); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.WriteString(e.TaskID.String()); err != nil {
		return err
	}

	if err = w.WriteByte('\n'); err != nil {
		return err
	}

	return nil
}

// Dequed represents a message emitted as a reply to a successful Deque request.
type Dequed struct {
	Request Request // The client request.
	Task    Task    // The task that is dequeued.
}

func (d Dequed) writeTo(w *bufio.Writer) (err error) {
	if _, err = w.WriteString(d.Request.String()); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.Write(ProtoRepDeQed); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.Write([]byte(d.Task.Queue)); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.WriteString(d.Task.ID.String()); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.WriteString(strconv.Itoa(len(d.Task.Body))); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.Write(d.Task.Body); err != nil {
		return err
	}

	if err = w.WriteByte('\n'); err != nil {
		return err
	}

	return nil
}

// Acked represents a message emitted as a reply to a successful Ack request.
type Acked struct {
	Request Request // The client request.
	Queue   Queue   // The queue.
	TaskID  TaskID  // The ID of the acknowledged task.
}

func (a Acked) writeTo(w *bufio.Writer) (err error) {
	if _, err = w.WriteString(a.Request.String()); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.Write(ProtoRepAcked); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.Write([]byte(a.Queue)); err != nil {
		return err
	}

	if err = w.WriteByte(' '); err != nil {
		return err
	}

	if _, err = w.WriteString(a.TaskID.String()); err != nil {
		return err
	}

	if err = w.WriteByte('\n'); err != nil {
		return err
	}

	return nil
}

type writerTo interface {
	writeTo(*bufio.Writer) error
}

// Error is a message replied when there is an error handling a request.
type Error struct {
	Request        // The client request.
	Message string // The error message.
}

func (e *Error) Error() string {
	return fmt.Sprintf("taskq error for request %v: %v", e.Request, e.Message)
}

func (e *Error) writeTo(w *bufio.Writer) error {
	if _, err := w.WriteString(e.Request.String()); err != nil {
		return err
	}

	if err := w.WriteByte(' '); err != nil {
		return err
	}

	if _, err := w.Write(ProtoRepError); err != nil {
		return err
	}

	if err := w.WriteByte(' '); err != nil {
		return err
	}

	if _, err := w.WriteString(e.Message); err != nil {
		return err
	}

	if err := w.WriteByte('\n'); err != nil {
		return err
	}

	return nil
}

// Timeout represents a timeout message.
type Timeout time.Time

// EnQHandler handles Enque messages.
type EnQHandler struct{}

func (h EnQHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	enq := msg.Data().(Enque)
	dict := ctx.Dict(active)

	next := TaskID(1)
	if v, err := dict.Get("_next_"); err == nil {
		next = v.(TaskID)
	}

	key := next.String()
	task := Task{
		Queue: enq.Queue,
		ID:    next,
		Body:  enq.Body,
	}
	if err := dict.Put(key, task); err != nil {
		return err
	}

	if err := dict.Put("_next_", next+1); err != nil {
		return err
	}

	enqued := Enqued{
		Request: enq.Request,
		Queue:   enq.Queue,
		TaskID:  next,
	}
	return ctx.ReplyTo(msg, enqued)
}

func (h EnQHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	// Send the Enque message to the bee that owns queues/q.
	q := string(msg.Data().(Enque).Queue)
	return beehive.MappedCells{{queues, q}}
}

// doDequeTask adds a task to the dequed dictionary and replys to the message.
func doDequeTask(msg beehive.Msg, ctx beehive.RcvContext, t Task) error {
	ctx.ReplyTo(msg, Dequed{
		Request: msg.Data().(Deque).Request,
		Task:    t,
	})

	ddict := ctx.Dict(dequed)
	return ddict.Put(t.ID.String(), dqTask{
		Task:      t,
		DequeTime: time.Now(),
	})
}

// DeQHandler handles Deque messages.
type DeQHandler struct{}

func (h DeQHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	// Lookup tasks in the out of order dictionary.
	odict := ctx.Dict(ooo)

	var key string
	odict.ForEach(func(k string, v interface{}) (next bool) {
		key = k
		return false
	})

	if key != "" {
		v, err := odict.Get(key)
		if err != nil {
			return err
		}
		odict.Del(key)
		return doDequeTask(msg, ctx, v.(Task))
	}

	// If the out of order dictionary had no tasks,
	// try dequeueing from the active dictionary.
	adict := ctx.Dict(active)

	first := TaskID(1)
	if v, err := adict.Get("_first_"); err == nil {
		first = v.(TaskID)
	}

	firstKey := first.String()
	v, err := adict.Get(firstKey)
	if err != nil {
		return ErrEmptyQueue
	}
	adict.Del(firstKey)
	adict.Put("_first_", first+1)

	return doDequeTask(msg, ctx, v.(Task))
}

func (h DeQHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	// Send the Deque message to the bee that owns queues/q.
	q := string(msg.Data().(Deque).Queue)
	return beehive.MappedCells{{queues, q}}
}

// AckHandler handles Ack messages.
type AckHandler struct{}

func (h AckHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	ack := msg.Data().(Ack)
	acked := Acked{
		Request: ack.Request,
		Queue:   ack.Queue,
		TaskID:  ack.TaskID,
	}

	key := ack.TaskID.String()
	ddict := ctx.Dict(dequed)
	if err := ddict.Del(key); err == nil {
		ctx.ReplyTo(msg, acked)
		return nil
	}

	// The task might have been moved from dequed to out of order, because of a
	// timeout. So, we need to search the active dictionary as well.
	odict := ctx.Dict(ooo)
	if err := odict.Del(key); err == nil {
		ctx.ReplyTo(msg, acked)
		return nil
	}

	return ErrNoSuchTask
}

func (h AckHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	// Send the Ack message to the bee that owns queues/q.
	q := string(msg.Data().(Ack).Queue)
	return beehive.MappedCells{{queues, q}}
}

// TimeoutHandler handles Ack messages.
type TimeoutHandler struct {
	// ExpDur is the duration after which a dequed and unacknowledged task
	// is returned to the out-of-order dictionary.
	ExpDur time.Duration
}

func (h TimeoutHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	tout := time.Time(msg.Data().(Timeout))
	ddict := ctx.Dict(dequed)

	var expiredKeys []string
	odict := ctx.Dict(ooo)
	ddict.ForEach(func(k string, v interface{}) (next bool) {
		t := v.(dqTask)
		if tout.Sub(t.DequeTime) < h.ExpDur {
			return true
		}

		key := t.ID.String()
		odict.Put(key, t.Task)
		expiredKeys = append(expiredKeys, key)
		return true
	})

	for _, k := range expiredKeys {
		ddict.Del(k)
	}

	return nil
}

func (h TimeoutHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {
	// Broadcast the timeout message to all local bees.
	return beehive.MappedCells{}
}

type httpHandler struct {
	Hive beehive.Hive // Hive represents the hive our handler is registered on.
}

// EnQHTTPHandler provides the HTTP endpoint for enqueuing tasks.
type EnQHTTPHandler httpHandler

func (h *EnQHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q, ok := mux.Vars(r)["queue"]
	if !ok {
		http.Error(w, "unkown queue", http.StatusBadRequest)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}

	e := Enque{
		Queue: Queue(q),
		Body:  b,
	}
	ctx, cnl := context.WithTimeout(context.Background(), httpTimeout)
	defer cnl()

	res, err := h.Hive.Sync(ctx, e)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch res := res.(type) {
	case Error:
		http.Error(w, res.Message, http.StatusInternalServerError)
	case Enqued:
		fmt.Fprintf(w, "task %v enqueued\n", res.TaskID)
	}
}

// DeQHTTPHandler provides the HTTP endpoint for dequeuing tasks.
type DeQHTTPHandler httpHandler

func (h *DeQHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q, ok := mux.Vars(r)["queue"]
	if !ok {
		http.Error(w, "unkown queue", http.StatusBadRequest)
		return
	}

	ctx, cnl := context.WithTimeout(context.Background(), httpTimeout)
	defer cnl()

	d := Deque{Queue: Queue(q)}
	res, err := h.Hive.Sync(ctx, d)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch res := res.(type) {
	case Error:
		http.Error(w, res.Message, http.StatusInternalServerError)
	case Dequed:
		if err = json.NewEncoder(w).Encode(res.Task); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// AckHTTPHandler provides the HTTP endpoint for acknowledging tasks.
type AckHTTPHandler httpHandler

func (h *AckHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	q, ok := vars["queue"]
	if !ok {
		http.Error(w, "unkown queue", http.StatusBadRequest)
		return
	}

	t, ok := vars["id"]
	if !ok {
		http.Error(w, "unkown task", http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseUint(t, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid task id: %v", err),
			http.StatusBadRequest)
		return
	}

	ctx, cnl := context.WithTimeout(context.Background(), httpTimeout)
	defer cnl()

	a := Ack{
		TaskID: TaskID(id),
		Queue:  Queue(q),
	}
	res, err := h.Hive.Sync(ctx, a)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch res := res.(type) {
	case Error:
		http.Error(w, res.Message, http.StatusInternalServerError)
	case Acked:
		fmt.Fprintf(w, "task %v acknowledged\n", id)
	}
}

const (
	CmdLen = 3
	ResLen = 5
)

var (
	ProtoCmdEnQ = []byte("enq")
	ProtoCmdDeQ = []byte("deq")
	ProtoCmdAck = []byte("ack")

	ProtoRepEnQed = []byte("enqed")
	ProtoRepDeQed = []byte("deqed")
	ProtoRepAcked = []byte("acked")
	ProtoRepError = []byte("error")
)

type ProtoHandler struct {
	lis  net.Listener
	done chan struct{}
}

func NewProtoHandler(addr string) (h *ProtoHandler, err error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	h = &ProtoHandler{
		lis:  lis,
		done: make(chan struct{}),
	}
	return h, nil
}

func (h *ProtoHandler) Start(ctx beehive.RcvContext) {
	defer close(h.done)

	glog.Infof("taskq is listening on: %s", h.lis.Addr())

	for {
		c, err := h.lis.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				continue
			}
			return
		}

		// TODO(soheil): do we need to be graceful for connections?
		go ctx.StartDetached(h.NewConnHandler(c))
	}
}

func (h *ProtoHandler) Stop(ctx beehive.RcvContext) {
	h.lis.Close()
	<-h.done
}

func (h *ProtoHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	return errors.New("protohandler: received unexpected message")
}

type ConnHandler struct {
	conn net.Conn
	done chan struct{}
	w    *bufio.Writer
}

func (h *ProtoHandler) NewConnHandler(conn net.Conn) *ConnHandler {
	return &ConnHandler{
		conn: conn,
		done: make(chan struct{}),
	}
}

func (h *ConnHandler) Start(ctx beehive.RcvContext) {
	r := bufio.NewReader(h.conn)
	h.w = bufio.NewWriter(h.conn)

	var req Request
	var cmd []byte
	var err error

	defer func() {
		h.conn.Close()
		if err != nil && err != io.EOF {
			if _, ok := err.(net.Error); !ok {
				glog.Errorf("error in connection handler: %v", err)
			}
		}
		close(h.done)
	}()

	for {
		if req, err = h.readRequestID(r); err != nil {
			return
		}

		if cmd, err = h.readCmd(r); err != nil {
			// TODO(soheil): respond with error.
			return
		}

		switch {
		case bytes.Equal(cmd, ProtoCmdEnQ):
			err = h.handleEnQ(req, ctx, r)

		case bytes.Equal(cmd, ProtoCmdDeQ):
			err = h.handleDeQ(req, ctx, r)

		case bytes.Equal(cmd, ProtoCmdAck):
			err = h.handleAck(req, ctx, r)

		default:
			err = ErrInvalidCmd
		}

		if err != nil {
			return
		}
	}
}

func (h *ConnHandler) readRequestID(r *bufio.Reader) (req Request, err error) {
	reqs, err := r.ReadString(' ')
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseUint(reqs[:len(reqs)-1], 10, 64)
	return Request(id), err
}

func (h *ConnHandler) readCmd(r *bufio.Reader) (cmd []byte, err error) {
	cmd = make([]byte, CmdLen)
	if n, err := io.ReadAtLeast(r, cmd, CmdLen); n != CmdLen {
		if err == io.EOF {
			return nil, err
		}
		return nil, ErrInvalidCmd
	}

	if c, err := r.ReadByte(); err != nil && c != ' ' {
		return nil, ErrInvalidCmd
	}

	return cmd, nil
}

func (h *ConnHandler) handleEnQ(req Request, ctx beehive.RcvContext,
	r *bufio.Reader) error {

	q, err := r.ReadString(' ')
	if err != nil {
		return err
	}
	q = q[:len(q)-1]

	lstr, err := r.ReadString(' ')
	if err != nil {
		return err
	}

	l, err := strconv.Atoi(string(lstr[:len(lstr)-1]))
	if err != nil {
		return err
	}

	var d []byte
	d = make([]byte, l)
	n, err := io.ReadAtLeast(r, d, l)
	if err != nil {
		return err
	}
	if n != l {
		return ErrNotEnoughData
	}

	if err := h.readNewLine(r); err != nil {
		return err
	}

	ctx.Emit(Enque{
		Request: req,
		Queue:   Queue(q),
		Body:    d,
	})
	return nil
}

func (h *ConnHandler) readNewLine(r *bufio.Reader) error {
	for i := 0; i < 2; i++ {
		c, err := r.ReadByte()
		if err != nil && err != io.EOF {
			return err
		}
		if c == '\n' {
			return nil
		}
		if i == 0 && c == '\r' {
			continue
		}
		return ErrNoNewLine
	}

	return ErrNoNewLine
}

// dropNewLine is an efficient and minimal function to remove a trailing new
// line from the string.
func dropNewLine(str string) string {
	l := len(str)
	if l == 0 {
		return ""
	}

	if str[l-1] != '\n' {
		return str
	}

	if l == 1 {
		return ""
	}

	if str[l-2] == '\r' {
		return str[:l-2]
	}

	return str[:l-1]
}

func (h *ConnHandler) handleDeQ(req Request, ctx beehive.RcvContext,
	r *bufio.Reader) error {

	q, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	q = dropNewLine(q)

	ctx.Emit(Deque{
		Request: req,
		Queue:   Queue(q),
	})
	return nil
}

func (h *ConnHandler) handleAck(req Request, ctx beehive.RcvContext,
	r *bufio.Reader) error {

	q, err := r.ReadString(' ')
	if err != nil {
		return err
	}
	q = q[:len(q)-1]

	s, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	s = dropNewLine(s)

	tid, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}

	ctx.Emit(Ack{
		Request: req,
		Queue:   Queue(q),
		TaskID:  TaskID(tid),
	})
	return nil
}

func (h *ConnHandler) Stop(ctx beehive.RcvContext) {
	h.conn.Close()
	<-h.done
}

func (h *ConnHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	res := msg.Data().(writerTo)
	if err := res.writeTo(h.w); err != nil {
		return err
	}
	return h.w.Flush()
}

var repl = args.NewInt(args.Flag("taskq.repl", 3,
	"replication factor of taskq"))
var addr = args.NewString(args.Flag("taskq.addr", ":7979",
	"listening address of taskq server"))
var rate = args.NewUint64(args.Flag("taskq.maxrate", uint64(bucket.Unlimited),
	"maximum message rate of each client"))

// Option represents taskq server options.
type Option args.V

// ReplicationFactor is an option represeting the replication factor of taskq.
func ReplicationFactor(f int) Option { return Option(repl(f)) }

// Address is an option representing a taskq address.
func Address(a string) Option { return Option(addr(a)) }

// MaxRate is an option represeting the maximum rate of messages a client
// can send per second.
func MaxRate(r uint64) Option { return Option(rate(r)) }

// RegisterTaskQ registers the taskq application and all its handler in the
// hive.
func RegisterTaskQ(h beehive.Hive, opts ...Option) error {
	if !flag.Parsed() {
		flag.Parse()
	}

	proto, err := NewProtoHandler(addr.Get(opts))
	if err != nil {
		return err
	}

	r := rate.Get(opts)
	taskq := h.NewApp("taskq", beehive.Persistent(repl.Get(opts)),
		beehive.OutRate(bucket.Rate(r), 2*r))
	taskq.Handle(Enque{}, EnQHandler{})
	taskq.Handle(Deque{}, DeQHandler{})
	taskq.Handle(Ack{}, AckHandler{})
	taskq.Handle(Timeout{}, TimeoutHandler{
		ExpDur: 60 * time.Second,
	})

	ah := &AckHTTPHandler{Hive: h}
	taskq.HandleHTTP("/{queue}/tasks/{id:[0-9]+}", ah).Methods("DELETE")
	dh := &DeQHTTPHandler{Hive: h}
	taskq.HandleHTTP("/{queue}/tasks/deque", dh).Methods("POST")
	eh := &EnQHTTPHandler{Hive: h}
	taskq.HandleHTTP("/{queue}/tasks", eh).Methods("POST")

	taskq.Detached(beehive.NewTimer(30*time.Second, func() {
		h.Emit(Timeout(time.Now()))
	}))
	taskq.Detached(proto)

	return nil
}

func init() {
	gob.Register(Acked{})
	gob.Register(Ack{})
	gob.Register(Dequed{})
	gob.Register(Deque{})
	gob.Register(Enqued{})
	gob.Register(Enque{})
	gob.Register(Queue(""))
	gob.Register(Request(0))
	gob.Register(TaskID(0))
	gob.Register(Task{})
	gob.Register(dqTask{})
}
