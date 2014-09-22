package bh

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net/http"

	"github.com/golang/glog"
)

var client = &http.Client{}

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
