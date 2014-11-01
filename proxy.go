package beehive

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/connpool"
)

const (
	defaultMaxRetries = 5
	defaultBackoff    = 50 * time.Millisecond
)

func newHttpClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Dial: (&connpool.Dialer{
				Dialer:         net.Dialer{Timeout: timeout},
				MaxConnPerHost: 64,
			}).Dial,
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConnsPerHost: 64,
		},
		// TODO(soheil): maybe only go1.3?
		// Timeout: timeout,
	}
}

type proxyBee struct {
	localBee
	proxy *proxy
}

type proxy struct {
	client *http.Client

	to       string
	stateURL string
	msgURL   string
	cmdURL   string
	raftURL  string

	lastRTT     time.Duration
	avgRTT      time.Duration
	backoffStep time.Duration
	maxRetries  uint32
}

func newProxy(client *http.Client, addr string) *proxy {
	return newProxyWithRetry(client, addr, 0, 1)
}

func newProxyWithRetry(client *http.Client, addr string,
	backoffStep time.Duration, maxRetries uint32) *proxy {
	// TODO(soheil): add scheme.
	return &proxy{
		client:      client,
		to:          addr,
		stateURL:    buildURL("http", addr, serverV1StatePath),
		msgURL:      buildURL("http", addr, serverV1MsgPath),
		cmdURL:      buildURL("http", addr, serverV1CmdPath),
		raftURL:     buildURL("http", addr, serverV1RaftPath),
		backoffStep: backoffStep,
		maxRetries:  maxRetries,
	}
}

type clientMethod func() (*http.Response, error)

func (p *proxy) doWithRetry(method clientMethod) (*http.Response, error) {
	var err error
	var res *http.Response
	backoff := p.backoffStep
	for retries := uint32(0); retries < p.maxRetries; retries++ {
		start := time.Now()
		res, err = method()
		if err != nil {
			if retries == p.maxRetries-1 {
				break
			}

			glog.V(2).Infof("error in communicting with %v: %v", p.to, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		p.lastRTT = time.Now().Sub(start)
		if p.avgRTT == 0 {
			p.avgRTT = p.lastRTT
		} else {
			p.avgRTT = (9*p.lastRTT + p.avgRTT) / 10
		}
		return res, err
	}
	glog.Errorf("cannot communicate with %v (%v retries): %v", p.to, p.maxRetries,
		err)
	return nil, err
}

func (p *proxy) post(url string, bodyType string, body io.Reader) (
	*http.Response, error) {

	return p.doWithRetry(func() (*http.Response, error) {
		return p.client.Post(url, bodyType, body)
	})
}

func (p *proxy) get(url string) (resp *http.Response, err error) {
	return p.doWithRetry(func() (*http.Response, error) {
		return p.client.Get(url)
	})
}

func (p *proxy) sendMsg(m *msg) error {
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(m); err != nil {
		return err
	}

	res, err := p.post(p.msgURL, "application/x-gob", &data)
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

func (p *proxy) sendCmd(c *cmd) (interface{}, error) {
	// TODO(soheil): We need to add a retry strategy here.
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(c); err != nil {
		return nil, err
	}

	glog.V(2).Infof("Proxy to %v sends command %v", p.to, c)
	pRes, err := p.post(p.cmdURL, "application/x-gob", &data)
	if err != nil {
		return nil, err
	}
	glog.V(2).Infof("Proxy to %v receives the result for command %v", p.to, c)

	defer pRes.Body.Close()
	if pRes.StatusCode != http.StatusOK {
		var b bytes.Buffer
		b.ReadFrom(pRes.Body)
		return nil, errors.New(string(b.Bytes()))
	}
	cRes := cmdResult{}
	if err := gob.NewDecoder(pRes.Body).Decode(&cRes); err != nil {
		return nil, err
	}
	return cRes.get()
}

// TODO(soheil): We should batch here.
func (b *proxyBee) handleMsg(mh msgAndHandler) {
	mh.msg.MsgTo = b.ID()

	glog.V(2).Infof("Proxy %v sends msg %v", b, mh.msg)
	if err := b.proxy.sendMsg(mh.msg); err != nil {
		glog.Errorf("cannot send message %v to %v: %v", mh.msg, b, err)
	}
}

func (b *proxyBee) handleCmd(cc cmdAndChannel) {
	switch cc.cmd.Data.(type) {
	case cmdStop, cmdStart:
		b.localBee.handleCmd(cc)
	default:
		d, err := b.proxy.sendCmd(&cc.cmd)
		if cc.ch != nil {
			cc.ch <- cmdResult{Data: d, Err: err}
		}
	}
}

func (p proxy) doSendRaft(url string, m raftpb.Message) error {
	// TODO(soheil): this should get a slice of messages and send them through the
	// pipe. But becuase pb does not support encoding of multiple entries, we send
	// them one by one for now.
	d, err := m.Marshal()
	if err != nil {
		glog.Fatalf("cannot marshal raft message")
	}

	r, err := p.post(url, "application/x-protobuf", bytes.NewBuffer(d))
	if err != nil {
		return err
	}

	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		var b bytes.Buffer
		b.ReadFrom(r.Body)
		return errors.New(string(b.Bytes()))
	}
	return nil
}

func (p proxy) sendRaft(m raftpb.Message) error {
	glog.V(2).Infof("proxy to %v sends raft message %v", p.to, m)
	return p.doSendRaft(p.raftURL, m)
}

func (p proxy) sendBeeRaft(app string, b uint64, m raftpb.Message) error {
	url := buildURL("http", p.to,
		fmt.Sprintf("%s/%s/%v", serverV1RaftPath, app, b))
	glog.V(2).Infof("proxy to %v sends bee raft message %v", url, m)
	return p.doSendRaft(url, m)
}

func (p proxy) state() (hiveState, error) {
	s := hiveState{}
	r, err := p.get(p.stateURL)
	if err != nil {
		return s, err
	}

	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		var b bytes.Buffer
		b.ReadFrom(r.Body)
		return s, errors.New(string(b.Bytes()))
	}

	dec := json.NewDecoder(r.Body)
	err = dec.Decode(&s)
	return s, err
}

// TODO(soheil): Maybe start should return an error.
func (b *proxyBee) start() {
	b.status = beeStatusStarted
	glog.V(2).Infof("%v started", b)

	for b.status == beeStatusStarted {
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

func (b *proxyBee) String() string {
	return "proxy " + b.localBee.String()
}
