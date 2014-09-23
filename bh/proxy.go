package bh

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/golang/glog"
)

var client = &http.Client{
	Transport: &http.Transport{
		Dial: (&ConnPoolDialer{
			MaxConnPerAddr: 64,
		}).Dial,
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConnsPerHost: 64,
	},
}

const (
	DefaultMaxConnsPerHost = 10
)

type DialFunc func(network, addr string) (net.Conn, error)

// ConnPoolDialer is an http dialer that uses a capped connection pool to bound
// the number of parallel connections towards each address.
type ConnPoolDialer struct {
	sync.Mutex
	conns map[netAndAddr]*connPool

	// Same as http.Transport.Dial. If nil, net.Dial is used.
	DialFunc DialFunc

	// The maximum number of connections we can pool per address. If it is set to
	// 0 we use DefaultMaxConnsPerHost.
	MaxConnPerAddr int
}

type netAndAddr struct {
	net  string
	addr string
}

func (p *ConnPoolDialer) pool(network, addr string) *connPool {
	p.Lock()
	defer p.Unlock()

	max := p.MaxConnPerAddr
	if max == 0 {
		max = DefaultMaxConnsPerHost
	}

	if p.conns == nil {
		p.conns = make(map[netAndAddr]*connPool)
	}

	pool, ok := p.conns[netAndAddr{network, addr}]
	if !ok {
		pool = &connPool{
			connCh: make(chan *pooledConn),
			tokens: max,
		}
		p.conns[netAndAddr{network, addr}] = pool
	}

	return pool
}

func (p *ConnPoolDialer) Dial(network, addr string) (net.Conn, error) {
	pool := p.pool(network, addr)

	dial := p.DialFunc
	if dial == nil {
		dial = (&net.Dialer{}).Dial
	}

	conn, err := pool.maybeDial(network, addr, dial)
	if err != nil {
		return nil, err
	}

	if err == nil && conn != nil {
		return conn, nil
	}

	for {
		select {
		case conn := <-pool.connCh:
			return conn, nil
		case <-time.After(10 * time.Millisecond):
			conn, err := pool.maybeDial(network, addr, dial)
			if err != nil {
				return nil, err
			}

			if err == nil && conn != nil {
				return conn, nil
			}
		}
	}
}

type connPool struct {
	sync.Mutex

	connCh chan *pooledConn // Used to wait for a new free connection.
	tokens int              // Cap minus the number of open connections.
}

func (p *connPool) maybeDial(network, addr string, d DialFunc) (net.Conn,
	error) {

	if p.getToken() != 0 {
		conn, err := d(network, addr)
		if err != nil {
			return conn, err
		}

		pConn := &pooledConn{
			Conn: conn,
			pool: p,
		}
		return pConn, nil
	}

	return nil, nil
}

func (p *connPool) getToken() int {
	p.Lock()
	defer p.Unlock()

	t := p.tokens
	if t > 0 {
		p.tokens--
	}
	return t
}

func (p *connPool) putToken() int {
	p.Lock()
	defer p.Unlock()

	t := p.tokens
	p.tokens++
	return t
}

type pooledConn struct {
	net.Conn
	pool *connPool
}

func (c *pooledConn) Close() error {
	select {
	case c.pool.connCh <- c:
		return nil
	default:
		c.pool.putToken()
		return c.Conn.Close()
	}
}

type proxyBee struct {
	localBee
	proxy proxy
}

type proxy struct {
	to     HiveID
	msgURL string
	cmdURL string
}

func NewProxy(to HiveID) proxy {
	return proxy{
		to:     to,
		msgURL: to.msgURL(),
		cmdURL: to.cmdURL(),
	}
}

func (id HiveID) msgURL() string {
	return fmt.Sprintf("http://%s/hive/v1/msg", id)
}

func (id HiveID) cmdURL() string {
	return fmt.Sprintf("http://%s/hive/v1/cmd", id)
}

func (p proxy) SendMsg(m *msg) error {
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(m); err != nil {
		return err
	}

	res, err := client.Post(p.msgURL, "application/x-gob", &data)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		b.ReadFrom(res.Body)
		return errors.New(string(b.Bytes()))
	}
	return nil
}

func (p proxy) SendCmd(c *RemoteCmd) (interface{}, error) {
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(c); err != nil {
		return nil, err
	}

	glog.V(2).Infof("Proxy to %v sends command %v", p.to, c)
	pRes, err := client.Post(p.cmdURL, "application/x-gob", &data)
	if err != nil {
		return nil, err
	}

	defer pRes.Body.Close()
	if pRes.StatusCode != http.StatusOK {
		var b bytes.Buffer
		b.ReadFrom(pRes.Body)
		return nil, errors.New(string(b.Bytes()))
	}
	cRes := CmdResult{}
	if err := gob.NewDecoder(pRes.Body).Decode(&cRes); err != nil {
		return nil, err
	}
	return cRes.get()
}

// TODO(soheil): We should batch here.
func (b *proxyBee) handleMsg(mh msgAndHandler) {
	mh.msg.MsgTo = b.id()

	glog.V(2).Infof("Proxy %v sends msg %v", b.id(), mh.msg)
	if err := b.proxy.SendMsg(mh.msg); err != nil {
		glog.Errorf("Cannot send message %v to %v: %v", mh.msg, b.id(), err)
	}
}

// TODO(soheil): Maybe start should return an error.
func (b *proxyBee) start() {
	b.stopped = false
	glog.V(2).Infof("Proxy started for %v", b.id())

	for !b.stopped {
		select {
		case d, ok := <-b.dataCh:
			if !ok {
				return
			}
			b.handleMsg(d)

		case c, ok := <-b.ctrlCh:
			if !ok {
				return
			}
			b.handleCmd(c)
		}
	}
}
