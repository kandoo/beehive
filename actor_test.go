package actor

import (
  "fmt"
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

func (h *MyHandler) Map(m Msg, c Context) MapSet {
  return MapSet{{}}
}

func (h *MyHandler) Recv(m Msg, c RecvContext) {
  fmt.Println("Received")
}

func TestStage(t *testing.T) {
  cfg := StageConfig{
    StageAddr:     "0.0.0.0:7737",
    RegAddr:       "127.0.0.1:1234",
    DataChBufSize: 1024,
  }
  stage := NewStage(cfg)
  actor := stage.NewActor("MyActor")
  actor.Handle(MyMsg{}, &MyHandler{})

  waitCh := make(chan interface{})
  go stage.Start(waitCh)

  stage.Emit(&MyMsg{val: 1})
  <-waitCh
}
