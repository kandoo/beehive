package main

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"google.golang.org/grpc"

	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"

	grpchello "github.com/grpc/grpc-common/go/helloworld"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/bradfitz/http2"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/soheilhy/cmux"
)

type exampleHTTPHandler struct{}

func (h *exampleHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "example http response")
}

func serveHTTP2(l net.Listener) {
	s := &http.Server{
		Handler: &exampleHTTPHandler{},
	}
	s2 := &http2.Server{}

	for {
		c, err := l.Accept()
		if err != nil {
			if terr, ok := err.(net.Error); ok && terr.Temporary() {
				continue
			}

			return
		}

		go s2.HandleConn(s, c, s.Handler)
	}
}

func serveHTTP(l net.Listener) {
	s := &http.Server{
		Handler: &exampleHTTPHandler{},
	}
	s.Serve(l)
}

type ExampleRPCRcvr struct{}

func (r *ExampleRPCRcvr) Cube(i int, j *int) error {
	*j = i * i
	return nil
}

func serveRPC(l net.Listener) {
	s := rpc.NewServer()
	s.Register(&ExampleRPCRcvr{})
	s.Accept(l)
}

type grpcServer struct{}

func (s *grpcServer) SayHello(ctx context.Context, in *grpchello.HelloRequest) (
	*grpchello.HelloReply, error) {

	return &grpchello.HelloReply{Message: "Hello " + in.Name + " from cmux"}, nil
}

func serveGRPC(l net.Listener) {
	grpcs := grpc.NewServer()
	grpchello.RegisterGreeterServer(grpcs, &grpcServer{})
	grpcs.Serve(l)
}

type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}

func main() {
	l, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	certificate, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}

	const requiredCipher = tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
	config := &tls.Config{
		CipherSuites:       []uint16{requiredCipher},
		InsecureSkipVerify: true,
		NextProtos:         []string{http2.NextProtoTLS, "h2-14"},
		Certificates:       []tls.Certificate{certificate},
	}
	config.Rand = rand.Reader

	tlsl := tls.NewListener(tcpKeepAliveListener{l.(*net.TCPListener)}, config)

	m := cmux.New(tlsl)

	// We first match the connection against HTTP2 fields. If matched, the
	// connection will be sent through the "grpcl" listener.
	grpcl := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	// Otherwise, we match it againts HTTP1 methods and HTTP2. If matched by
	// any of them, it is sent through the "httpl" listener.
	http2l := m.Match(cmux.HTTP2())

	httpl := m.Match(cmux.HTTP1Fast())
	// If not matched by HTTP, we assume it is an RPC connection.
	rpcl := m.Match(cmux.Any())

	// Then we used the muxed listeners.
	go serveGRPC(grpcl)
	go serveHTTP2(http2l)
	go serveHTTP(httpl)
	go serveRPC(rpcl)

	m.Serve()
}
