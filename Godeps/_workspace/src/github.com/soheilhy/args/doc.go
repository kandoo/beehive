// args is a generic library for optional arguments. It is
// inspired by Dave Cheney's functional options idea
// (http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis).
// It can serve the purpose of Python "kwargs" for Go programs.
//
// Optional arguments are defined using New and its typed variants:
//
//
// These arguments are basically functions that return argument values
// of type args.V. To use these argument values the function receives
// a variadic list of args.V and then get the value of each argument
// from those values:
//
//		var RoundTripper = args.New()
//		var Timeout = args.NewDuration()
//		func MyServer(args ...args.V) {
//			rt := RoundTripper.Get(args)
//			to := Timeout.Get(args)
//			...
//		}
//		MyServer()
//		MyServer(Timeout(1 * time.Second))
//		MyServer(Timeout(2 * time.Second), RoundTripper(MyTransport))
//
// To use typed arguments, instead of the generic args.V,
// you'll need to write a few lines of boiler-plates:
//
//		var roundTripper = args.New()
//		var timeout = args.NewDuration()
//
//		type ServerOpt args.V
//		func RoundTripper(r http.RoundTripper) ServerOpt {
//			return ServerOpt(roundTripper(r))
//		}
//		func Timeout(d time.Duration) ServerOpt {
//			return ServerOpt(d)
//		}
//		func MyServer(opts ...ServerOpt) {
//			rt := roundTripper.Get(opts).(http.RoundTripper)
//			to := timeout.Get(opts)
//			...
//		}
//
// Note that, args is focused on easy-to-use APIs. It is not efficient
// and is wasteful if the function is frequently invoked.
package args
