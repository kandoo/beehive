package beehive

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net/http"

	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/soheilhy/beehive/connpool"
)

var client = &http.Client{
	Transport: &http.Transport{
		Dial: (&connpool.Dialer{
			MaxConnPerAddr: 64,
		}).Dial,
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConnsPerHost: 64,
	},
}

type proxyBee struct {
	localBee
	proxy proxy
}

type proxy struct {
	to     string
	msgURL string
	cmdURL string
}

func newProxyWithAddr(addr string) proxy {
	p := proxy{
		to:     addr,
		msgURL: msgURL(addr),
		cmdURL: cmdURL(addr),
	}
	return p
}

func (h *hive) newProxy(to uint64) (proxy, error) {
	a, err := h.hiveAddr(to)
	if err != nil {
		return proxy{}, err
	}

	return newProxyWithAddr(a), nil
}

func msgURL(addr string) string {
	return fmt.Sprintf(serverV1MsgFormat, addr)
}

func cmdURL(addr string) string {
	return fmt.Sprintf(serverV1CmdFormat, addr)
}

func (p proxy) sendMsg(m *msg) error {
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

func (p proxy) sendCmd(c *cmd) (interface{}, error) {
	// TODO(soheil): We need to add a retry strategy here.
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
		glog.Errorf("Cannot send message %v to %v: %v", mh.msg, b, err)
	}
}

// TODO(soheil): Maybe start should return an error.
func (b *proxyBee) start() {
	b.stopped = false
	glog.V(2).Infof("Proxy started for %v", b)

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
