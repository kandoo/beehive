package beehive

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

const (
	defaultMaxRetries = 5
	defaultBackoff    = 50 * time.Millisecond
)

type proxy struct {
	client *http.Client

	to         string
	stateURL   string
	msgURL     string
	cmdURL     string
	raftURL    string
	beeRaftURL string

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
		beeRaftURL:  buildURL("http", addr, serverV1BeeRaftPath),
		backoffStep: backoffStep,
		maxRetries:  maxRetries,
	}
}

type clientMethod func() (*http.Response, error)

func (p *proxy) sendRaft(buf io.Reader) error {
	res, err := p.do("POST", p.raftURL, "application/x-raft", buf)
	maybeCloseResponse(res)
	return err
}

func (p *proxy) sendBeeRaft(buf io.Reader) error {
	res, err := p.do("POST", p.beeRaftURL, "application/x-raft", buf)
	maybeCloseResponse(res)
	return err
}

func (p *proxy) sendCmdNew(buf io.Reader) (*http.Response, error) {
	return p.do("POST", p.cmdURL, "application/x-raft", buf)
}

func (p *proxy) sendMsgNew(buf io.Reader) error {
	res, err := p.do("POST", p.msgURL, "application/x-raft", buf)
	maybeCloseResponse(res)
	return err
}

func (p *proxy) do(method, urlStr, bodyType string, body io.Reader) (
	res *http.Response, err error) {

	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)

	backoff := p.backoffStep
	for retries := uint32(0); retries < p.maxRetries; retries++ {
		start := time.Now()
		res, err = p.client.Do(req)
		if err != nil {
			if retries == p.maxRetries-1 {
				break
			}

			glog.Errorf("error in communicting with %v: %v", p.to, err)
			glog.Errorf("retrying %v in %v", p.to, backoff)
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

func (p proxy) state() (hiveState, error) {
	s := hiveState{}

	r, err := p.do("GET", p.stateURL, "", nil)
	if err != nil {
		return s, err
	}

	defer maybeCloseResponse(r)
	if r.StatusCode != http.StatusOK {
		var b bytes.Buffer
		b.ReadFrom(r.Body)
		return s, errors.New(string(b.Bytes()))
	}

	dec := json.NewDecoder(r.Body)
	err = dec.Decode(&s)
	return s, err
}

func maybeCloseResponse(r *http.Response) {
	if r == nil {
		return
	}
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()
}
