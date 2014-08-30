package bh

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"strconv"
	"testing"
)

const (
	handlers int = 10
	msgs         = 1000000
)

var testHiveCh chan interface{} = make(chan interface{})

type MyMsg int

type MyHandler struct{}

func (h *MyHandler) Map(m Msg, c MapContext) MapSet {
	v := int(m.Data().(MyMsg))
	return MapSet{{"D", Key(strconv.Itoa(v % handlers))}}
}

func intToBytes(i int) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	return buf
}

func bytesToInt(b []byte) int {
	return int(binary.LittleEndian.Uint64(b))
}

func (h *MyHandler) Rcv(m Msg, c RcvContext) {
	hash := int(m.Data().(MyMsg)) % handlers
	d := c.State().Dict("D")
	k := Key(strconv.Itoa(hash))
	v, err := d.Get(k)
	i := 1
	if err == nil {
		i += bytesToInt(v)
	}

	if err = d.Put(k, intToBytes(i)); err != nil {
		panic(fmt.Sprintf("Cannot change the key: %v", err))
	}

	id := c.(*rcvContext).bee.id().Id % uint32(handlers)
	if id != uint32(hash) {
		panic(fmt.Sprintf("Invalid message %v received in %v.", m, id))
	}

	if i == msgs/handlers {
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

	for i := 1; i <= msgs; i++ {
		hive.Emit(MyMsg(i))
	}

	for i := 0; i < handlers; i++ {
		<-testHiveCh
	}

	hive.Stop()
	<-hiveWCh
}
