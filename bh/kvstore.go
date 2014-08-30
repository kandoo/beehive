package bh

import (
	"errors"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
)

type persistentStateManager struct {
	sync.Mutex
	hive   *hive
	dbs    map[AppName][]*bolt.DB
	nextId int
}

func newPersistentStateManager(h *hive) *persistentStateManager {
	return &persistentStateManager{
		hive: h,
		dbs:  make(map[AppName][]*bolt.DB),
	}
}

func (m *persistentStateManager) closeDBs() {
	for _, dbs := range m.dbs {
		for _, db := range dbs {
			db.Close()
		}
	}
}

func (m *persistentStateManager) newAppDB(a *app) (*bolt.DB, error) {
	m.Lock()
	defer m.Unlock()

	m.nextId++

	path := filepath.Join(
		m.hive.config.DBDir, string(a.Name())+"-"+strconv.Itoa(m.nextId))
	glog.V(1).Infof("Creating/Opening the db file for %s in %s.", a.Name(), path)
	db, err := bolt.Open(path, 0600, nil)
	if err == nil {
		m.dbs[a.Name()] = append(m.dbs[a.Name()], db)
	}
	return db, err
}

type persistentState struct {
	db   *bolt.DB
	txCh chan interface{}
}

func (m *persistentStateManager) newState(a *app) *persistentState {
	db, err := m.newAppDB(a)
	if err != nil {
		glog.Fatalf("Error in opening the persistent state store: %v", err)
	}

	return &persistentState{
		db: db,
	}
}

type boltCmd struct {
	rCh chan<- interface{}
}

type boltGet struct {
	boltCmd
	dict DictionaryName
	key  Key
}

type boltGetResult struct {
	val Value
	err error
}

type boltPut struct {
	boltCmd
	dict DictionaryName
	key  Key
	val  Value
}

type boltDel struct {
	boltCmd
	dict DictionaryName
	key  Key
}

type boltIterate struct {
	boltCmd
	dict DictionaryName
	fn   IterFn
}

type boltTxCommit struct {
	boltCmd
}

type boltTxAbort struct {
	boltCmd
}

func (s *persistentState) maybeBeginTx(readOnly bool) bool {
	if s.txCh != nil {
		return false
	}

	s.txCh = make(chan interface{})
	loop := func(tx *bolt.Tx) error {
		for {
			switch cmd := (<-s.txCh).(type) {
			case boltGet:
				b, err := tx.CreateBucketIfNotExists([]byte(cmd.dict))
				if err != nil {
					cmd.rCh <- err
					continue
				}
				cmd.rCh <- b.Get([]byte(cmd.key))
			case boltPut:
				b, err := tx.CreateBucketIfNotExists([]byte(cmd.dict))
				if err != nil {
					cmd.rCh <- err
					continue
				}
				cmd.rCh <- b.Put([]byte(cmd.key), []byte(cmd.val))
			case boltDel:
				b, err := tx.CreateBucketIfNotExists([]byte(cmd.dict))
				if err != nil {
					cmd.rCh <- err
					continue
				}
				cmd.rCh <- b.Delete([]byte(cmd.key))
			case boltIterate:
				b, err := tx.CreateBucketIfNotExists([]byte(cmd.dict))
				if err != nil {
					cmd.rCh <- err
					continue
				}

				b.ForEach(func(k []byte, v []byte) error {
					cmd.fn(Key(k), Value(v))
					return nil
				})
				cmd.rCh <- true
			case boltTxCommit:
				defer func() {
					cmd.rCh <- true
				}()
				return nil
			case boltTxAbort:
				defer func() {
					cmd.rCh <- false
				}()
				return errors.New("Abort")
			}
		}
		return nil
	}

	if readOnly {
		go s.db.View(loop)
	} else {
		go s.db.Update(loop)
	}

	return true
}

func (s *persistentState) BeginTx() error {
	if s.maybeBeginTx(false) {
		return nil
	}

	return errors.New("There is an active transaction on this state.")
}

func (s *persistentState) CommitTx() error {
	if s.txCh == nil {
		return errors.New("There is no active transactions.")
	}

	ch := make(chan interface{})
	s.txCh <- boltTxCommit{boltCmd{ch}}
	<-ch
	s.txCh = nil
	return nil
}

func (s *persistentState) AbortTx() error {
	if s.txCh == nil {
		return errors.New("There is no active transactions.")
	}
	ch := make(chan interface{})
	s.txCh <- boltTxAbort{boltCmd{ch}}
	<-ch
	s.txCh = nil
	return nil
}

func (s *persistentState) get(d DictionaryName, k Key) (Value, error) {
	if s.maybeBeginTx(false) {
		defer s.CommitTx()
	}

	rCh := make(chan interface{})
	s.txCh <- boltGet{
		boltCmd: boltCmd{rCh},
		dict:    d,
		key:     k,
	}

	switch v := (<-rCh).(type) {
	case []byte:
		if v == nil {
			return nil, errors.New("No value found for this key.")
		}

		return v, nil

	case error:
		return nil, v

	case nil:
		return nil, nil
	}

	panic("Invalid result received from the channel.")
}

func (s *persistentState) put(d DictionaryName, k Key, v Value) error {
	newTx := s.maybeBeginTx(false)

	rCh := make(chan interface{})
	s.txCh <- boltPut{
		dict:    d,
		key:     k,
		val:     v,
		boltCmd: boltCmd{rCh},
	}

	err := <-rCh
	if newTx {
		if err != nil {
			s.AbortTx()
		} else {
			s.CommitTx()
		}
	}

	if err != nil {
		return err.(error)
	}

	return nil
}

func (s *persistentState) del(d DictionaryName, k Key) error {
	newTx := s.maybeBeginTx(false)

	rCh := make(chan interface{})
	s.txCh <- boltDel{
		boltCmd: boltCmd{rCh},
		dict:    d,
		key:     k,
	}

	err := (<-rCh).(error)
	if newTx {
		if err != nil {
			s.AbortTx()
		} else {
			s.CommitTx()
		}
	}
	return err
}

func (s *persistentState) forEach(d DictionaryName, f IterFn) {
	if s.maybeBeginTx(false) {
		defer s.CommitTx()
	}

	rCh := make(chan interface{})
	s.txCh <- boltIterate{
		boltCmd: boltCmd{rCh},
		dict:    d,
		fn:      f,
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
