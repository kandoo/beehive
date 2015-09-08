package beehive_test

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"net"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

// HelloDetached represents a message in our hello world example.
type HelloDetached struct {
	Name string // Name is the name of the person saying hello.
}

// HelloCount represents a message sent as a reply to a HelloDetached.
type HelloCount struct {
	Name  string // Name of the person.
	Count int    // Number of times we have said hello.
}

// RcvfDetached receives the message and the context.
func RcvfDetached(msg beehive.Msg, ctx beehive.RcvContext) error {
	// msg is an envelope around the Hello message.
	// You can retrieve the Hello, using msg.Data() and then
	// you need to assert that its a Hello.
	hello := msg.Data().(HelloDetached)
	// Using ctx.Dict you can get (or create) a dictionary.
	dict := ctx.Dict("hello_dict")
	// Using Get(), you can get the value associated with
	// a key in the dictionary. Keys are always string
	// and values are generic interface{}'s.
	v, err := dict.Get(hello.Name)
	// If there is an error, the entry is not in the
	// dictionary. Otherwise, we set cnt based on
	// the value we already have in the dictionary
	// for that name.
	cnt := 0
	if err == nil {
		cnt = v.(int)
	}
	// Now we increment the count.
	cnt++
	// Reply to the message with the count of hellos.
	ctx.Reply(msg, HelloCount{Name: hello.Name, Count: cnt})
	// Finally we update the count stored in the dictionary.
	return dict.Put(hello.Name, cnt)
}

// HelloListener is a detached handler that acts as a newtork listener for
// our example.
type HelloListener struct {
	lis net.Listener
}

// NewHelloListener creates a new HelloListener.
func NewHelloListener() *HelloListener {
	lis, err := net.Listen("tcp", ":6789")
	if err != nil {
		glog.Fatalf("cannot start listener: %v", err)
	}

	return &HelloListener{lis: lis}
}

// Start is called once the detached handler starts.
func (h *HelloListener) Start(ctx beehive.RcvContext) {
	defer h.lis.Close()

	for {
		c, err := h.lis.Accept()
		if err != nil {
			return
		}
		// Start a new detached handler for the connection.
		go ctx.StartDetached(&HelloConn{conn: c})
	}
}

// Stop is called when the hive is stopping.
func (h *HelloListener) Stop(ctx beehive.RcvContext) {
	h.lis.Close()
}

// Rcv receives replies to HelloListener which we do not expect to receive.
// Note that HelloConn emits hellos and should receives replies.
func (h *HelloListener) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	return errors.New("unexpected message")
}

// HelloConn is a detached handler that handles a connection.
type HelloConn struct {
	conn net.Conn
}

// Start is called once the detached handler starts.
func (h *HelloConn) Start(ctx beehive.RcvContext) {
	defer h.conn.Close()

	r := bufio.NewReader(h.conn)
	for {
		name, _, err := r.ReadLine()
		if err != nil {
			return
		}
		ctx.Emit(HelloDetached{Name: string(name)})
	}
}

// Stop is called when the hive is stopping.
func (h *HelloConn) Stop(ctx beehive.RcvContext) {
	h.conn.Close()
}

// Rcv receives HelloCount messages.
func (h *HelloConn) Rcv(msg beehive.Msg, ctx beehive.RcvContext) error {
	reply := msg.Data().(HelloCount)
	_, err := fmt.Fprintf(h.conn, "hello %s (%d)!\n", reply.Name, reply.Count)
	return err
}

func Example_detached() {
	// Create the hello world application and make sure .
	app := beehive.NewApp("hello-world", beehive.Persistent(1))
	// Register the handler for Hello messages.
	app.HandleFunc(HelloDetached{},
		beehive.RuntimeMap(RcvfDetached), RcvfDetached)
	// Register the detached handler for the hello world listener.
	app.Detached(NewHelloListener())
	// Start the DefaultHive.
	beehive.Start()
}

func init() {
	// We need to register HelloCount on gob.
	gob.Register(HelloCount{})
}
