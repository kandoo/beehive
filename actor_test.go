package actor

import (
  "fmt"
  "log"
  "runtime"
  "strconv"
  "testing"

  "github.com/coreos/go-etcd/etcd"
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
  //for i := 1; i < 10000; i++ {
  //}
  //fmt.Println("Received %v", c.(*recvContext).rcvr.id())
}

func BenchmarkStage(b *testing.B) {
  runtime.GOMAXPROCS(4)

  cfg := StageConfig{
    StageAddr:     "0.0.0.0:7737",
    RegAddrs:      []string{"127.0.0.1:1234"},
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

func TestEtcd(t *testing.T) {
  client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
  resp, err := client.Create("/actors/123/123/x", "12", 100)
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("%#v \n", *resp.Node)

  resp, err = client.CreateInOrder("/actors/123/123/y/", "12", 100)
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("%#v \n", *resp.Node)

  resp, err = client.Get("/actors/123/123/y", false, false)
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("%#v \n", *resp.Node)
  for _, n := range resp.Node.Nodes {
    log.Printf("%s: %s\n", n.Key, n.Value)
  }
}
