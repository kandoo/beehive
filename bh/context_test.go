package bh

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
)

type testLockMsg int

func (m testLockMsg) String() string {
	return strconv.Itoa(int(m))
}

func testLockFromValue(v Value) testLockMsg {
	i, _ := strconv.Atoi(string(v))
	return testLockMsg(i)
}

type testLockHandler struct {
}

const testLockDict = DictName("S")

var testLockCh = make(chan error)

func (l *testLockHandler) Rcv(msg Msg, ctx RcvContext) error {
	data := msg.Data().(testLockMsg)

	dict := ctx.Dict(testLockDict)

	v, err := dict.Get("Last")
	if err == nil && data == 1 {
		testLockCh <- errors.New("Received 1 with a dirty dictionary")
		return nil
	}

	if err != nil && data == 2 {
		testLockCh <- errors.New("Received 2 in a wrong bee")
		return nil
	}

	if data == 2 && testLockFromValue(v) != 2 {
		testLockCh <- fmt.Errorf("Invalid state when received 2: %d",
			testLockFromValue(v))
		return nil
	}

	switch data {
	case 1:
		data++
		testLockCh <- ctx.Lock(MappedCells{{testLockDict, Key(data.String())}})
	case 2:
		testLockCh <- nil
	}

	dict.Put("Last", Value(data.String()))
	return nil
}

func (l *testLockHandler) Map(msg Msg, ctx MapContext) MappedCells {
	d := msg.Data().(testLockMsg)
	return MappedCells{{testLockDict, Key(d.String())}}
}

func TestLock(t *testing.T) {
	h := NewHive()
	app := h.NewApp("TestLockHandler")
	app.Handle(testLockMsg(0), &testLockHandler{})

	joinCh := make(chan bool)
	go h.Start(joinCh)

	defer func() {
		h.Stop()
		<-joinCh
	}()

	h.Emit(testLockMsg(1))

	err := <-testLockCh
	if err != nil {
		t.Error(err)
		return
	}

	h.Emit(testLockMsg(2))

	err = <-testLockCh
	if err != nil {
		t.Error(err)
	}
}
