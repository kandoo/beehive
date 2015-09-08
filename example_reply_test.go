package beehive_test

import (
	"fmt"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
)

// HelloReply represents a message in our hello world example.
type HelloReply struct {
	Name string // Name is the name of the person saying hello.
}

// RcvfReply receives the message and the context.
func RcvfReply(msg beehive.Msg, ctx beehive.RcvContext) error {
	// msg is an envelope around the Hello message.
	// You can retrieve the Hello, using msg.Data() and then
	// you need to assert that its a Hello.
	hello := msg.Data().(HelloReply)
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
	ctx.Reply(msg, cnt)
	// Finally we update the count stored in the dictionary.
	return dict.Put(hello.Name, cnt)
}

func Example_reply() {
	// Create the hello world application and make sure .
	app := beehive.NewApp("hello-world", beehive.Persistent(1))
	// Register the handler for Hello messages.
	app.HandleFunc(HelloReply{}, beehive.RuntimeMap(RcvfReply), RcvfReply)

	// Start the default hive.
	go beehive.Start()
	defer beehive.Stop()

	name := "your name"
	for i := 0; i < 2; i++ {
		// Sync sends the Hello message and waits until it receives the reply.
		res, err := beehive.Sync(context.TODO(), HelloReply{Name: name})
		if err != nil {
			glog.Fatalf("error in sending Hello: %v", err)
		}
		cnt := res.(int)
		fmt.Printf("hello %s (%d)!\n", name, cnt)
	}
}
