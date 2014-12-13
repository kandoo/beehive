package connpool

import (
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	// DefaultMaxConnsPerHost is the default number of connections towards an
	// address.
	DefaultMaxConnsPerHost = 10
)

var (
	// ErrTimeout represents that no connection could be grabbed from the pool
	// after the connection timeout.
	ErrTimeout = errTimeout{}
)

type errTimeout struct{}

func (err errTimeout) Error() string {
	return "dial timeout error"
}

func (err errTimeout) Temporary() bool {
	return true
}

func (err errTimeout) Timeout() bool {
	return true
}

type DialFunc func(network, addr string) (net.Conn, error)

// Dialer is an http dialer that uses a capped connection pool to bound
// the number of parallel connections towards each address.
type Dialer struct {
	sync.Mutex
	conns map[netAndAddr]bucket
	// MaxConnPerHost is the maximum number of parallel connections dialed for
	// each host. If it is set to 0 we use DefaultMaxConnsPerHost.
	MaxConnPerHost int
	// Dialer is the underlying network dialer.
	Dialer net.Dialer
}

type netAndAddr struct {
	net  string
	addr string
}

func (d *Dialer) bucket(network, addr string) bucket {
	d.Lock()

	max := d.MaxConnPerHost
	if max == 0 {
		max = DefaultMaxConnsPerHost
	}

	if d.conns == nil {
		d.conns = make(map[netAndAddr]bucket)
	}

	b, ok := d.conns[netAndAddr{network, addr}]
	if !ok {
		b = make(bucket, max)
		for i := 0; i < max; i++ {
			b <- struct{}{}
		}
		d.conns[netAndAddr{network, addr}] = b
	}

	d.Unlock()
	return b
}

func (d *Dialer) Dial(network, addr string) (net.Conn, error) {
	b := d.bucket(network, addr)
	return b.dial(network, addr, d.Dialer)
}

type bucket chan struct{}

func (b bucket) dial(network, addr string, dialer net.Dialer) (net.Conn, error) {

	select {
	case <-b:
		c, err := dialer.Dial(network, addr)
		if err != nil {
			b <- struct{}{}
			return c, err
		}
		pc := &conn{
			Conn:   c,
			bucket: b,
		}
		return pc, nil

	case <-time.After(dialer.Timeout):
		return nil, ErrTimeout
	}
}

type conn struct {
	net.Conn
	bucket bucket
}

func (c *conn) Close() error {
	c.bucket <- struct{}{}
	return c.Close()
}

// NewHTTPClient creates an HTTP client, with the given timeout. Unlike
// the http package, this method does not allow more than maxConnPerHost
// connections towards each remote host.
func NewHTTPClient(maxConnPerHost int, timeout time.Duration) *http.Client {
	return newHTTPClient(maxConnPerHost, timeout)
}
