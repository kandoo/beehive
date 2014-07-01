package actor

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
)

type MyMsg struct {
	GenericMsg
	val int
}

func (msg *MyMsg) Type() MsgType {
	return "actor.MyMsg"
}

type MyHandler struct{}

var i int = 1

func (h *MyHandler) Map(m Msg, c Context) MapSet {
	i++
	return MapSet{{"D", Key(strconv.Itoa(m.(*MyMsg).val % 10))}}
}

func (h *MyHandler) Recv(m Msg, c RecvContext) {
}

func BenchmarkStage(b *testing.B) {
	runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(1)

	cfg := StageConfig{
		StageAddr:     "0.0.0.0:7737",
		RegAddrs:      []string{"127.0.0.1:4001"},
		DataChBufSize: 1024,
	}
	stage := NewStage(cfg)
	actor := stage.NewActor("MyActor")
	actor.Handle(MyMsg{}, &MyHandler{})

	waitCh := make(chan interface{})
	go stage.Start(waitCh)

	for i := 1; i < 1000000; i++ {
		stage.Emit(&MyMsg{val: i})
	}

	<-waitCh
	fmt.Println("Done!")
}
