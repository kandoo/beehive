package beehive_test

import (
	"fmt"
	"net/http"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
)

// HelloHTTP represents a message in our hello world example.
type HelloHTTP struct {
	Name string // Name is the name of the person saying hello.
}

// RcvfHTTP receives the message and the context.
func RcvfHTTP(msg beehive.Msg, ctx beehive.RcvContext) error {
	// msg is an envelope around the Hello message.
	// You can retrieve the Hello, using msg.Data() and then
	// you need to assert that its a Hello.
	hello := msg.Data().(HelloHTTP)
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

type HelloHTTPHandler struct{}

func (h *HelloHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name, ok := vars["name"]
	if !ok {
		http.Error(w, "no name", http.StatusBadRequest)
		return
	}

	res, err := beehive.Sync(context.TODO(), HelloHTTP{Name: name})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "hello %s (%d)\n", name, res.(int))
}

func Example_hTTP() {
	// Create the hello world application and make sure .
	app := beehive.NewApp("hello-world", beehive.Persistent(1))
	// Register the handler for Hello messages.
	app.HandleFunc(HelloHTTP{}, beehive.RuntimeMap(RcvfHTTP), RcvfHTTP)
	// Register the HTTP handler for the hello world application.
	app.HandleHTTP("/{name}", &HelloHTTPHandler{}).Methods("POST")
	// Start the DefaultHive.
	beehive.Start()
}
