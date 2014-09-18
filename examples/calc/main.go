package main

import (
	"fmt"
	"strconv"

	"github.com/soheilhy/beehive/bh"
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
func (c *Calculator) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	op := msg.Data().(Op)
	return bh.MappedCells{
		{"Op", bh.Key(strconv.Itoa(int(op.OpT)))},
	}
}

func (c *Calculator) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	op := msg.Data().(Op)
	res := c.calc(op)
	fmt.Printf("%d %s %d = %d\n", op.Lhs, op.OpT, op.Rhs, res)
	ctx.ReplyTo(msg, res)
	return nil
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

type Generator struct{}

func (g *Generator) Start(ctx bh.RcvContext) {
	fmt.Println("Generator started.")

	ctx.Emit(Op{1, 2, Add})
	fmt.Println("Emitted add.")

	ctx.Emit(Op{1, 2, Sub})
	fmt.Println("Emitted sub.")

	ctx.Emit(Op{1, 2, Add})
	fmt.Println("Emitted add.")

	ctx.Emit(Op{1, 2, Sub})
	fmt.Println("Emitted sub.")
}

func (g *Generator) Stop(ctx bh.RcvContext) {
	fmt.Println("Generator stopped.")
}

func (g *Generator) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	fmt.Printf("The result is: %d\n", msg.Data().(int))
	return nil
}

func main() {
	h := bh.NewHive()
	a := h.NewApp("Calc")
	a.Handle(Op{}, &Calculator{})
	a.Detached(&Generator{})

	joinCh := make(chan bool)
	go func() {
		fmt.Println("Stage started.")
		h.Start()
		joinCh <- true
	}()

	<-joinCh
	fmt.Println("Stage stopped.")
}
