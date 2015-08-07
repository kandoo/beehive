package args_test

import (
	"fmt"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/soheilhy/args"
)

var ListenOn = args.NewInt(args.Default(8080))
var BufferSize = args.NewUint64(args.Default(uint64(1024 * 1024)))
var StateDir = args.NewString(args.Flag("test.state.dir", "/tmp", "state dir"))

func Server(opts ...args.V) {
	port := ListenOn.Get(opts)
	bufs := BufferSize.Get(opts)
	sdir := StateDir.Get(opts)

	fmt.Printf("port=%d buf=%d state=%s\n", port, bufs, sdir)
}

func Example() {
	Server()
	Server(ListenOn(80), BufferSize(2048*1024), StateDir("/tmp2"))
	// Output:
	//port=8080 buf=1048576 state=/tmp
	//port=80 buf=2097152 state=/tmp2
}
