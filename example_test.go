package beehive_test

import "github.com/kandoo/beehive"

// Hello represents a message in our hello world example.
type Hello struct {
	Name string // Name is the name of the person saying hello.
}

// Rcvf receives the message and the context.
func Rcvf(msg beehive.Msg, ctx beehive.RcvContext) error {
	// msg is an envelope around the Hello message.
	// You can retrieve the Hello, using msg.Data() and then
	// you need to assert that its a Hello.
	hello := msg.Data().(Hello)
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
	// And then we print the hello message.
	ctx.Printf("hello %s (%d)!\n", hello.Name, cnt)
	// Finally we update the count stored in the dictionary.
	return dict.Put(hello.Name, cnt)
}

func Example() {
	// Create the hello world application and make sure .
	app := beehive.NewApp("hello-world", beehive.Persistent(1))
	// Register the handler for Hello messages.
	app.HandleFunc(Hello{}, beehive.RuntimeMap(Rcvf), Rcvf)
	// Emit simply emits a message, here a
	// string of your name.
	go beehive.Emit(Hello{Name: "your name"})
	// Emit another message with the same name
	// to test the counting feature.
	go beehive.Emit(Hello{Name: "your name"})
	// Start the DefualtHive.
	beehive.Start()
}
