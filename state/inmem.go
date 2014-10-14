package state

import (
	"bytes"
	"encoding/gob"
	"errors"
)

var (
	ErrTxOpen = errors.New("State has an open transaction")
)

// InMem is a simple dictionary that uses in memory maps.
type InMem struct {
	Dicts map[string]*inMemDict
	tx    inMemTx
}

// NewInMem creates a new InMem state.
func NewInMem() *InMem {
	return &InMem{
		Dicts: make(map[string]*inMemDict),
	}
}

func (s *InMem) Save() ([]byte, error) {
	if s.tx.status == TxOpen {
		return nil, ErrTxOpen
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *InMem) Restore(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(s)
}

func (s *InMem) TxStatus() TxStatus {
	return s.tx.status
}

func (s *InMem) BeginTx() error {
	if s.tx.status == TxOpen {
		return errors.New("Transaction is already started")
	}

	s.maybeNewTransaction()
	s.tx.status = TxOpen
	return nil
}

func (s *InMem) maybeNewTransaction() {
	if s.tx.stage == nil {
		s.tx.state = s
		s.tx.stage = make(map[string]*inMemStagedDict)
	}
}

func (s *InMem) Tx() []Op {
	if s.tx.stage == nil {
		return nil
	}

	l := 0
	for _, dict := range s.tx.stage {
		l += len(dict.ops)
	}

	ops := make([]Op, 0, l)
	for _, dict := range s.tx.stage {
		for _, op := range dict.ops {
			ops = append(ops, op)
		}
	}
	return ops
}

func (s *InMem) CommitTx() error {
	if s.tx.status != TxOpen {
		return errors.New("No active transaction")
	}

	s.tx.Commit()
	if len(s.tx.stage) != 0 {
		s.tx.stage = make(map[string]*inMemStagedDict)
	}
	return nil
}

func (s *InMem) AbortTx() error {
	if s.tx.status != TxOpen {
		return errors.New("No active transaction")
	}

	s.tx.Abort()
	if len(s.tx.stage) != 0 {
		s.tx.stage = make(map[string]*inMemStagedDict)
	}
	return nil
}

func (s *InMem) Dict(name string) Dict {
	if s.tx.status == TxOpen {
		return s.tx.Dict(name)
	}

	return s.inMemDict(name)
}

func (s *InMem) inMemDict(name string) *inMemDict {
	d, ok := s.Dicts[name]
	if !ok {
		d = &inMemDict{name, make(map[string][]byte)}
		s.Dicts[name] = d
	}
	return d
}

type inMemDict struct {
	DictName string
	Dict     map[string][]byte
}

func (d inMemDict) Name() string {
	return d.DictName
}

func (d *inMemDict) Get(k string) ([]byte, error) {
	v, ok := d.Dict[k]
	if !ok {
		return v, errors.New("string does not exist.")
	}
	return v, nil
}

func (d *inMemDict) Put(k string, v []byte) error {
	d.Dict[k] = v
	return nil
}

func (d *inMemDict) Del(k string) error {
	delete(d.Dict, k)
	return nil
}

func (d *inMemDict) ForEach(f IterFn) {
	for k, v := range d.Dict {
		f(k, v)
	}
}

func (d *inMemDict) GetGob(k string, v interface{}) error {
	return GetGob(d, k, v)
}

func (d *inMemDict) PutGob(k string, v interface{}) error {
	return PutGob(d, k, v)
}

type inMemTx struct {
	state  *InMem
	stage  map[string]*inMemStagedDict
	status TxStatus
}

type inMemStagedDict struct {
	dict *inMemDict
	ops  map[string]Op
}

func (t *inMemTx) Dict(name string) Dict {
	d := &inMemStagedDict{
		dict: t.state.inMemDict(name),
		ops:  make(map[string]Op),
	}

	t.stage[name] = d
	return d
}

func (t *inMemTx) Commit() {
	for _, d := range t.stage {
		for _, o := range d.ops {
			switch o.T {
			case Put:
				d.dict.Put(o.K, o.V)
			case Del:
				d.dict.Del(o.K)
			}
		}
	}
	t.status = TxNone
}

func (t *inMemTx) Abort() {
	t.status = TxNone
	return
}

func (d *inMemStagedDict) Name() string {
	return d.dict.Name()
}

func (d *inMemStagedDict) Put(k string, v []byte) error {
	d.ops[k] = Op{
		T: Put,
		D: d.dict.Name(),
		K: k,
		V: v,
	}
	return nil
}

func (d *inMemStagedDict) Get(k string) ([]byte, error) {
	op, ok := d.ops[k]
	if ok {
		switch op.T {
		case Put:
			return op.V, nil
		case Del:
			return nil, errors.New("No such key")
		}
	}

	return d.dict.Get(k)
}

func (d *inMemStagedDict) Del(k string) error {
	d.ops[k] = Op{
		T: Del,
		D: d.dict.Name(),
		K: k,
	}
	return nil
}

func (d *inMemStagedDict) ForEach(f IterFn) {
	d.dict.ForEach(func(k string, v []byte) {
		op, ok := d.ops[k]
		if ok {
			switch op.T {
			case Put:
				f(op.K, op.V)
				return
			case Del:
				return
			}
		}

		f(k, v)
	})
}

func (d *inMemStagedDict) GetGob(k string, v interface{}) error {
	return GetGob(d, k, v)
}

func (d *inMemStagedDict) PutGob(k string, v interface{}) error {
	return PutGob(d, k, v)
}
