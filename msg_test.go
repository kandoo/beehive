package beehive

import (
	"sync"
	"testing"
)

func TestMsgChannelQueue(t *testing.T) {
	ch := newMsgChannel(7)
	ch.enque(msgAndHandler{msg: &msg{}})
	if _, ok := ch.deque(); !ok {
		t.Errorf("cannot deque")
	}

	for i := 0; i < 13; i++ {
		ch.enque(msgAndHandler{msg: &msg{MsgData: i}})
		if ch.len() != i+1 {
			t.Errorf("invalid len: actual=%v want=%v", ch.len(), i+1)
		}
	}

	for i := 0; i < 13; i++ {
		mh, ok := ch.deque()
		if !ok {
			t.Errorf("cannot deque the %v'th element", i)
		}
		if mh.msg.MsgData != i {
			t.Errorf("invalid data: actual=%v want=%v", mh.msg.MsgData, i)
		}
	}
}

func TestMsgChannel(t *testing.T) {
	sent := uint(1024 * 10)
	ch := newMsgChannel(sent / 10)
	in := ch.in()
	for i := uint(0); i < sent; i++ {
		in <- msgAndHandler{msg: &msg{MsgData: i}}
	}
	out := ch.out()
	for i := uint(0); i < sent; i++ {
		mh := <-out
		if mh.msg.MsgData != i {
			t.Errorf("invalid message: actual=%v want=%v", mh.msg.Data, i)
		}
	}
}

func TestMsgChannelParallel(t *testing.T) {
	sent := uint(1024 * 10)
	ch := newMsgChannel(sent / 10)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		in := ch.in()
		for i := uint(0); i < sent; i++ {
			in <- msgAndHandler{msg: &msg{MsgData: i}}
		}
		wg.Done()
	}()

	go func() {
		out := ch.out()
		for i := uint(0); i < sent; i++ {
			mh := <-out
			if mh.msg.MsgData != i {
				t.Errorf("invalid message: actual=%v want=%v", mh.msg.Data, i)
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

func BenchmarkMsgChannel(b *testing.B) {
	b.StopTimer()

	sent := uint(b.N)
	ch := newMsgChannel(1)
	wg := sync.WaitGroup{}
	wg.Add(2)

	b.StartTimer()
	go func() {
		in := ch.in()
		for i := uint(0); i < sent; i++ {
			in <- msgAndHandler{}
		}
		wg.Done()
	}()

	go func() {
		out := ch.out()
		for i := uint(0); i < sent; i++ {
			<-out
		}
		wg.Done()
	}()

	wg.Wait()
}
