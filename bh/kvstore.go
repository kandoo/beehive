package bh

import (
	"errors"
	"path/filepath"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
)

type persistentStateManager struct {
	sync.Mutex
	dbs map[AppName]*bolt.DB
}

func (m *persistentStateManager) appDB(a *app) (*bolt.DB, error) {
	m.Lock()
	defer m.Unlock()

	db, ok := m.dbs[a.Name()]
	if ok {
		return db, nil
	}

	path := filepath.Join(a.hive.config.DBDir, string(a.Name()))
	glog.V(1).Infof("Creating/Opening the db file for %s in %s.", a.Name(), path)
	db, err := bolt.Open(path, 0600, nil)
	if err == nil {
		m.dbs[a.Name()] = db
	}

	return db, err
}

type persistentState struct {
	db   *bolt.DB
	bee  *bee
	txCh chan interface{}
}

type boltGet struct {
	dict DictionaryName
	key  Key
	rCh  chan<- interface{}
}

type boltGetResult struct {
	val Value
	err error
}

type boltPut struct {
	dict DictionaryName
	key  Key
	val  Value
	rCh  chan<- interface{}
}

type boltDel struct {
	dict DictionaryName
	key  Key
	rCh  chan<- interface{}
}

type boltIterate struct {
	dict DictionaryName
	fn   IterFn
	rCh  chan<- interface{}
}

type boltTxCommit struct{}
type boltTxAbort struct{}

func (s *persistentState) maybeStartTx(readOnly bool) bool {
	if s.txCh != nil {
		return false
	}

	s.txCh = make(chan interface{})
	loop := func(tx *bolt.Tx) error {
		for {
			switch cmd := (<-s.txCh).(type) {
			case boltGet:
				cmd.rCh <- tx.Bucket([]byte(cmd.dict)).Get([]byte(cmd.key))
			case boltPut:
				cmd.rCh <- tx.Bucket([]byte(cmd.dict)).Put(
					[]byte(cmd.key), []byte(cmd.val))
			case boltDel:
				cmd.rCh <- tx.Bucket([]byte(cmd.dict)).Delete([]byte(cmd.key))
			case boltIterate:
				tx.Bucket([]byte(cmd.dict)).ForEach(func(k []byte, v []byte) error {
					cmd.fn(Key(k), Value(v))
					return nil
				})
				cmd.rCh <- true
			case boltTxCommit:
				return nil
			case boltTxAbort:
				return errors.New("Abort")
			}
		}
		return nil
	}

	if readOnly {
		s.db.View(loop)
	} else {
		s.db.Update(loop)
	}

	return true
}

func (s *persistentState) commit() error {
	if s.txCh == nil {
		return errors.New("There is no active transactions.")
	}

	s.txCh <- boltTxCommit{}
	return nil
}

func (s *persistentState) abort() error {
	if s.txCh == nil {
		return errors.New("There is no active transactions.")
	}

	s.txCh <- boltTxAbort{}
	return nil
}

func (s *persistentState) get(d DictionaryName, k Key) (Value, error) {
	if s.maybeStartTx(true) {
		defer s.commit()
	}

	rCh := make(chan interface{})
	s.txCh <- boltGet{
		dict: d,
		key:  k,
		rCh:  rCh,
	}

	v := (<-rCh).([]byte)
	if v == nil {
		return nil, errors.New("No value found for this key.")
	}

	return v, nil
}

func (s *persistentState) put(d DictionaryName, k Key, v Value) error {
	if s.maybeStartTx(false) {
		defer s.commit()
	}

	rCh := make(chan interface{})
	s.txCh <- boltPut{
		dict: d,
		key:  k,
		val:  v,
		rCh:  rCh,
	}

	return (<-rCh).(error)
}

func (s *persistentState) del(d DictionaryName, k Key) error {
	if s.maybeStartTx(false) {
		defer s.commit()
	}

	rCh := make(chan interface{})
	s.txCh <- boltDel{
		dict: d,
		key:  k,
		rCh:  rCh,
	}

	return (<-rCh).(error)
}

func (s *persistentState) forEach(d DictionaryName, f IterFn) {
	if s.maybeStartTx(true) {
		defer s.commit()
	}

	rCh := make(chan interface{})
	s.txCh <- boltIterate{
		dict: d,
		fn:   f,
		rCh:  rCh,
	}

	<-rCh
}

func (s *persistentState) Dict(n DictionaryName) Dictionary {
	return &persistentDict{
		state: s,
		name:  n,
	}
}

type persistentDict struct {
	state *persistentState
	name  DictionaryName
}

func (d *persistentDict) Get(k Key) (Value, error) {
	return d.state.get(d.name, k)
}

func (d *persistentDict) Put(k Key, v Value) error {
	return d.state.put(d.name, k, v)
}

func (d *persistentDict) Del(k Key) error {
	return d.state.del(d.name, k)
}

func (d *persistentDict) ForEach(f IterFn) {
	d.state.forEach(d.name, f)
}
