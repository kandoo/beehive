package server

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
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
	ErrEmptyQueue = errors.New("no task in queue")
	// ErrNoTask is returned when acking or accessing a task that does not exist.
	ErrNoSuchTask = errors.New("no such task")
)

// Queue represents a named queue.
type Queue string

// Task represents a task in a queue.
type Task struct {
	ID    uint64 `json:"id"`    // Task's globally unique ID assigned by taskq.
	Queue Queue  `json:"queue"` // Task's queue.
	Body  []byte `json:"body"`  // Task's client data.
}

// dqTask is a dequed task that has a timestamp in addition to the task.
type dqTask struct {
	Task
	DequeTime time.Time // When the task is dequed.
}

// Enqueue enqueus a task.
type Enque struct {
	Task // The task to be enqueued.
}

// Deque represents a message emitted to dequeue a task from a queue.
type Deque struct {
	Queue // The queue to dequeue a task from.
}

// Ack represents a message emitted to acknowledge a previously dequeued task.
type Ack struct {
	ID    uint64 // The task ID.
	Queue Queue  // The queue.
}

// Timeout represents a timeout message.
type Timeout time.Time

// idToKey converts an integer ID to a dictionary key.
func idToKey(id uint64) (key string) {
	return strconv.FormatUint(id, 16)
}

// keyToId converts a dictionary key to an integer ID.
func keyToId(key string) (id uint64) {
	id, _ = strconv.ParseUint(key, 16, 64)
	return id
}

// EnQHandler handles Enque messages.
type EnQHandler struct{}

func (h EnQHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	enq := msg.Data().(Enque)
	dict := ctx.Dict(active)

	next := uint64(1)
	if v, err := dict.Get("_next_"); err == nil {
		next = v.(uint64)
	}

	key := idToKey(next)
	enq.Task.ID = next
	if err := dict.Put(key, enq.Task); err != nil {
		return err
	}

	return dict.Put("_next_", next+1)
}

func (h EnQHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	// Send the Enque message to the bee that owns queues/q.
	q := string(msg.Data().(Enque).Queue)
	return beehive.MappedCells{{queues, q}}
}

// doDequeTask adds a task to the dequed dictionary and replys to the message.
func doDequeTask(msg beehive.Msg, ctx beehive.RcvContext, t Task) error {
	ctx.ReplyTo(msg, t)

	ddict := ctx.Dict(dequed)
	return ddict.Put(idToKey(t.ID), dqTask{
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

	first := uint64(1)
	if v, err := adict.Get("_first_"); err == nil {
		first = v.(uint64)
	}

	firstKey := idToKey(first)
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
	key := idToKey(ack.ID)

	ddict := ctx.Dict(dequed)
	if err := ddict.Del(key); err == nil {
		return nil
	}

	// The task might have been moved from dequed to out of order, because of a
	// timeout. So, we need to search the active dictionary as well.
	odict := ctx.Dict(ooo)
	if err := odict.Del(key); err == nil {
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

		key := idToKey(t.ID)
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
		Task: Task{
			Queue: Queue(q),
			Body:  b,
		},
	}
	ctx, cnl := context.WithTimeout(context.Background(), httpTimeout)
	defer cnl()

	if _, err := h.Hive.Sync(ctx, e); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "task enqueued\n")
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

	d := Deque{
		Queue: Queue(q),
	}
	v, err := h.Hive.Sync(ctx, d)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err = json.NewEncoder(w).Encode(v.(Task)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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

	i, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid task id: %v", err),
			http.StatusBadRequest)
		return
	}

	ctx, cnl := context.WithTimeout(context.Background(), httpTimeout)
	defer cnl()

	a := Ack{
		ID:    uint64(i),
		Queue: Queue(q),
	}
	if _, err := h.Hive.Sync(ctx, a); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "task acknowledged\n")
}

// RegisterTaskQ registers the taskq application and all its handler in the
// hive.
func RegisterTaskQ(h beehive.Hive) {
	a := h.NewApp("taskq", beehive.Persistent(3))
	a.Handle(Enque{}, EnQHandler{})
	a.Handle(Deque{}, DeQHandler{})
	a.Handle(Ack{}, AckHandler{})
	a.Handle(Timeout{}, TimeoutHandler{
		ExpDur: 60 * time.Second,
	})

	ah := &AckHTTPHandler{Hive: h}
	a.HandleHTTP("/{queue}/tasks/{id:[0-9]+}", ah).Methods("DELETE")
	dh := &DeQHTTPHandler{Hive: h}
	a.HandleHTTP("/{queue}/tasks/deque", dh).Methods("POST")
	eh := &EnQHTTPHandler{Hive: h}
	a.HandleHTTP("/{queue}/tasks", eh).Methods("POST")

	a.Detached(beehive.NewTimer(30*time.Second, func() {
		h.Emit(Timeout(time.Now()))
	}))
}

func init() {
	gob.Register(Queue(""))
	gob.Register(Task{})
	gob.Register(dqTask{})
}
