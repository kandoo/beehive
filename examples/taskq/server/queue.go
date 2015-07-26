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

	"golang.org/x/net/context"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
)

const (
	// active is the name of the active dictionary.
	active = "active"
	// dequed is the name of the dequed dictionary.
	dequed = "dequed"
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

// EnQHandler handles Enque messages.
type EnQHandler struct{}

func (h EnQHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	enq := msg.Data().(Enque)

	dict := ctx.Dict(active)
	key := string(enq.Task.Queue)

	var tasks TaskRing
	if val, err := dict.Get(key); err == nil {
		tasks = val.(TaskRing)
	}

	enq.Task.ID = tasks.Stats.Enque + 1
	tasks.Enque(enq.Task)

	return dict.Put(key, tasks)
}

func (h EnQHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	// Send the Enque message to the bee that owns the Queue's entry in
	// the active dictionary.
	q := string(msg.Data().(Enque).Queue)
	return beehive.MappedCells{{active, q}}
}

// DeQHandler handles Deque messages.
type DeQHandler struct{}

func (h DeQHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	deq := msg.Data().(Deque)

	adict := ctx.Dict(active)
	key := string(deq.Queue)

	var atasks TaskRing
	if val, err := adict.Get(key); err == nil {
		atasks = val.(TaskRing)
	}

	task, ok := atasks.Deque()
	if !ok {
		return ErrEmptyQueue
	}
	if err := adict.Put(key, atasks); err != nil {
		return err
	}

	ddict := ctx.Dict(dequed)
	var dtasks map[uint64]dqTask
	if val, err := ddict.Get(key); err == nil {
		dtasks = val.(map[uint64]dqTask)
	} else {
		dtasks = make(map[uint64]dqTask)
	}

	dtasks[task.ID] = dqTask{
		Task:      task,
		DequeTime: time.Now(),
	}

	if err := ddict.Put(key, dtasks); err != nil {
		return err
	}

	ctx.ReplyTo(msg, task)
	return nil
}

func (h DeQHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	// Send the Deque message to the bee that owns the Queue's entry in
	// the active and the dequed dictionaries.
	q := string(msg.Data().(Deque).Queue)
	return beehive.MappedCells{{active, q}, {dequed, q}}
}

// AckHandler handles Ack messages.
type AckHandler struct{}

func (h AckHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	ack := msg.Data().(Ack)

	ddict := ctx.Dict(dequed)
	key := string(ack.Queue)

	if val, err := ddict.Get(key); err == nil {
		dtasks := val.(map[uint64]dqTask)
		if _, ok := dtasks[ack.ID]; ok {
			delete(dtasks, ack.ID)
			return ddict.Put(key, dtasks)
		}
	}

	// The task might have been moved from dequed to active, because of a timeout.
	// So, we need to search the active dictionary as well.
	adict := ctx.Dict(active)
	var atasks TaskRing
	if val, err := adict.Get(key); err == nil {
		atasks = val.(TaskRing)
	}

	if ok := atasks.Remove(ack.ID); !ok {
		return ErrNoSuchTask
	}

	return adict.Put(key, atasks)
}

func (h AckHandler) Map(msg beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	// Send the Ack message to the bee that owns the Queue's entry in
	// the active and the dequed dictionaries.
	q := string(msg.Data().(Ack).Queue)
	return beehive.MappedCells{{active, q}, {dequed, q}}
}

// TimeoutHandler handles Ack messages.
type TimeoutHandler struct {
	// ExpDur is the duration after which a dequed and unacknowledged task
	// is returned to the active dictionary.
	ExpDur time.Duration
}

func (h TimeoutHandler) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	tout := time.Time(msg.Data().(Timeout))
	ddict := ctx.Dict(dequed)

	expired := make(map[string][]Task)
	ddict.ForEach(func(k string, v interface{}) {
		dtasks := v.(map[uint64]dqTask)
		for _, t := range dtasks {
			if tout.Sub(t.DequeTime) < h.ExpDur {
				continue
			}

			expired[k] = append(expired[k], t.Task)
		}
	})

	adict := ctx.Dict(active)
	for q, etasks := range expired {
		v, _ := ddict.Get(q)
		dtasks := v.(map[uint64]dqTask)
		v, _ = adict.Get(q)
		atasks := v.(TaskRing)

		for _, t := range etasks {
			delete(dtasks, t.ID)
			atasks.Enque(t)
		}

		if err := ddict.Put(q, dtasks); err != nil {
			return err
		}

		if err := adict.Put(q, atasks); err != nil {
			return err
		}
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
	gob.Register(map[uint64]dqTask{})
}
