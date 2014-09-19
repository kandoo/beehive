package bh

import "fmt"

type TxSeq uint64
type TxGeneration uint64

type TxStatus uint8

const (
	TxCommitted TxStatus = iota
	TxOpen               = iota
)

// Tx represents the side effects of an operation: messages emitted during the
// transaction as well as state operations.
type Tx struct {
	Seq        TxSeq
	Generation TxGeneration
	Msgs       []Msg
	Ops        []StateOp
	Status     TxStatus
}

func (t *Tx) AddMsg(msg Msg) {
	t.Msgs = append(t.Msgs, msg)
}

func (t *Tx) AddOp(op StateOp) {
	t.Ops = append(t.Ops, op)
}

func (t *Tx) IsOpen() bool {
	return t.Status == TxOpen
}

func (t *Tx) Reset() {
	t.Msgs = nil
	t.Ops = nil
	t.Status = TxCommitted
}

func (t Tx) String() string {
	return fmt.Sprintf("Tx %d/%d (msgs: %d, ops: %d, open: %v)",
		t.Generation, t.Seq, len(t.Msgs), len(t.Ops), t.Status == TxOpen)
}

type TxInfo struct {
	Generation    TxGeneration
	LastCommitted TxSeq
	LastBuffered  TxSeq
}
