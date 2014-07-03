package main

import (
	"fmt"
	"strconv"

	"github.com/soheilhy/actor/actor"
)

// Binary operator.
type Op struct {
	Lhs int
	Rhs int
	OpT OpType
}

type OpType int

const (
	Add OpType = iota
	Sub        = iota
)

func (t OpType) String() string {
	switch t {
	case Add:
		return "+"
	case Sub:
		return "-"
	}
	return "NoOp!"
}

// This is a simple calculator that can do addition and subtraction.
type Calculator struct{}

// Operations are mapped based on their type. With this mapping, additions are
// all handled on the same stage, and so are subtractions.
func (c *Calculator) Map(msg actor.Msg, ctx actor.Context) actor.MapSet {
	op := msg.Data().(Op)
	return actor.MapSet{
		{"Op", actor.Key(strconv.Itoa(int(op.OpT)))},
	}
}

func (c *Calculator) Recv(msg actor.Msg, ctx actor.RecvContext) {
	op := msg.Data().(Op)
	fmt.Printf("%d %s %d = %d\n", op.Lhs, op.OpT, op.Rhs, c.calc(op))
}

func (c *Calculator) calc(op Op) int {
	switch op.OpT {
	case Add:
		return op.Lhs + op.Rhs
	case Sub:
		return op.Lhs - op.Rhs
	}
	return 0
}

func main() {
	s := actor.NewStage()
	a := s.NewActor("Test")
	a.Handle(Op{}, &Calculator{})

	joinCh := make(chan interface{})
	go s.Start(joinCh)
	fmt.Println("Stage started.")

	s.Emit(Op{1, 2, Add})
	fmt.Println("Emitted add.")

	s.Emit(Op{1, 2, Sub})
	fmt.Println("Emitted sub.")

	<-joinCh
}
