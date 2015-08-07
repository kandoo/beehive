# Introduction ![Travis Build Status](https://api.travis-ci.org/soheilhy/args.svg?branch=master "Travis Build Status")
args is a generic library for optional arguments. It is
inspired by Dave Cheney's
[functional options idea](http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis).
It can also serve the purpose of Python "kwargs" for Go programs.

# Usage
Optional arguments are defined using `New` and its typed variants.
These arguments are basically functions that return argument values
of type `args.V`. To use these argument values, your function receives
a variadic list of `args.V` and then gets the value of each argument:

```go
var Port = args.NewInt()
var RoundTripper = args.New(Default(http.DefaultTransport))
var Timeout = args.NewDuration(Flag("timeout", 10*time.Second, "timeout"))

func MyServer(args ...args.V) {
	port := Port.Get(args)
	rt := RoundTripper.Get(args)
	to := Timeout.Get(args)
	...
}

MyServer()
MyServer(Timeout(1 * time.Second))
MyServer(Timeout(2 * time.Second), RoundTripper(MyTransport))
```

`args` can load default values from flags as well as user-defined
constants.

To use user-defined types instead of the generic `args.V`,
you need to write a few lines of boiler-plates:

```go
var roundTripper = args.New(http.DefaultTransport)
var timeout = args.NewDuration()

type ServerOpt args.V
func RoundTripper(r http.RoundTripper) ServerOpt { return ServerOpt(roundTripper(r)) }
func Timeout(d time.Duration) ServerOpt { return ServerOpt(d) }

func MyServer(opts ...ServerOpt) {
	rt := roundTripper.Get(opts).(http.RoundTripper)
	to := timeout.Get(opts)
	...
}
```

Note that, args is focused on easy-to-use APIs. It is not efficient
and is wasteful if the function is frequently invoked.

# API
* [Go Doc](https://godoc.org/github.com/soheilhy/args)
