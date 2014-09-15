package bh

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
