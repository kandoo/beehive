package args_test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/soheilhy/args"
)

// port is an integer argument that its default value is read from
// the "-example.typed.port" flag.
var port = args.NewInt(args.Flag("example.typed.port", 1234, "the port"))

// roundTripper is a generic argument that its default value is
// http.DefaultTransport.
var roundTripper = args.New(args.Default(http.DefaultTransport))

// timeout is a duration argument.
var timeout = args.NewDuration()

type ServerOpt args.V

// Port, RoundTripper, and Timeout respectively wrap port, roundTripper,
// and timeout to return ServerOpt instead of args.V.
func Port(p int) ServerOpt                       { return ServerOpt(port(p)) }
func RoundTripper(r http.RoundTripper) ServerOpt { return ServerOpt(roundTripper(r)) }
func Timeout(d time.Duration) ServerOpt          { return ServerOpt(timeout(d)) }

func MyServer(opts ...ServerOpt) {
	port := port.Get(opts)
	fmt.Printf("listening on port %v\n", port)

	rt := roundTripper.Get(opts).(http.RoundTripper)
	if rt == http.DefaultTransport {
		fmt.Println("using the default transport")
	} else {
		fmt.Println("using a user-provided round-tripper")
	}

	to := timeout.Get(opts)
	fmt.Printf("using a timeout of %v\n", to)
}

func Example_typed() {
	// If you run "go test -example.typed.port=2222" this test fails,
	// because the output is (correctly) different.
	MyServer(Timeout(1 * time.Second))
	// Output:
	//listening on port 1234
	//using the default transport
	//using a timeout of 1s
}
