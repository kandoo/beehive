package bh

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
)

type MyMsg int

type MyHandler struct{}

const (
	kHandlers int = 10
	kMsgs         = 1000000
)

func (h *MyHandler) Map(m Msg, c Context) MapSet {
	v := int(m.Data().(MyMsg))
	return MapSet{{"D", Key(strconv.Itoa(v % kHandlers))}}
}

var testHiveCh chan interface{} = make(chan interface{})

func (h *MyHandler) Recv(m Msg, c RecvContext) {
	hash := int(m.Data().(MyMsg)) % kHandlers
	d := c.State().Dict("D")
	k := Key(strconv.Itoa(hash))
	v, ok := d.Get(k)
	if !ok {
		v = 0
	}
	i := v.(int) + 1
	d.Set(k, i)

	id := c.(*recvContext).rcvr.id().Id % uint32(kHandlers)
	if id != uint32(hash) {
		panic(fmt.Sprintf("Invalid message %v received in %v.", m, id))
	}

	if i == kMsgs/kHandlers {
		testHiveCh <- true
	}
}

func TestHive(t *testing.T) {
	runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(1)

	testHiveCh = make(chan interface{})
	defer func() {
		close(testHiveCh)
		testHiveCh = nil
	}()

	hive := NewHive()
	app := hive.NewApp("MyApp")
	app.Handle(MyMsg(0), &MyHandler{})

	hiveWCh := make(chan interface{})
	go hive.Start(hiveWCh)

	for i := 1; i <= kMsgs; i++ {
		hive.Emit(MyMsg(i))
	}

	for i := 0; i < kHandlers; i++ {
		<-testHiveCh
	}

	hive.Stop()
	<-hiveWCh
}
