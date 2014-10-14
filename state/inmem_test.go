package state

import (
	"bytes"
	"testing"
)

func testInMemTx(t *testing.T, abort bool) {
	state := NewInMem()

	if err := state.BeginTx(); err != nil {
		t.Errorf("Error in tx begin: %v", err)
	}

	n1 := "TestDict1"
	d1 := state.Dict(n1)

	keys := []string{"k1", "k2"}
	vals := [][]byte{[]byte("v1"), []byte("v2")}

	for i := range keys {
		d1.Put(keys[i], vals[i])
		_, ok := state.Dicts[n1].Dict[keys[i]]
		if ok {
			t.Errorf("Key is inserted before commit: %s", keys[i])
		}

		v, err := d1.Get(keys[i])
		if err != nil {
			t.Errorf("Key cannot be read in the transaction: %s", keys[i])
		}

		if bytes.Compare(v, vals[i]) != 0 {
			t.Errorf("Invalid value for key %s: %s != %s", keys[i], v, vals[i])
		}
	}

	if abort {
		if err := state.AbortTx(); err != nil {
			t.Errorf("Cannot abort the transaction")
		}
	} else {
		if err := state.CommitTx(); err != nil {
			t.Errorf("Cannot commit the transaction")
		}
	}

	for i := range keys {
		v, err := state.Dicts[n1].Get(keys[i])
		if abort {
			if err == nil {
				t.Errorf("Key is inserted despite aborting the tx: %s", keys[i])
			}
			continue
		}

		if err != nil {
			t.Errorf("Key is not inserted after commit: %s", keys[i])
		}
		if bytes.Compare(v, vals[i]) != 0 {
			t.Errorf("Invalid value for key %s: %s != %s", keys[i], v, vals[i])
		}
	}

	if err := state.BeginTx(); err != nil {
		t.Errorf("Transaction is not correctly closed")
	}
}

func TestCommit(t *testing.T) {
	testInMemTx(t, false)
}

func TestAbort(t *testing.T) {
	testInMemTx(t, true)
}

func TestTxStatus(t *testing.T) {
	var state State = NewInMem()
	if state.BeginTx(); state.TxStatus() != TxOpen {
		t.Error("Tx status should be open")
	}

	if state.CommitTx(); state.TxStatus() != TxNone {
		t.Error("Tx status should be none")
	}

	if state.BeginTx(); state.TxStatus() != TxOpen {
		t.Error("Tx status should be open")
	}

	if state.AbortTx(); state.TxStatus() != TxNone {
		t.Error("Tx status should be none")
	}
}

func TestSave(t *testing.T) {
	state := NewInMem()
	state.BeginTx()
	if _, err := state.Save(); err == nil {
		t.Error("Should not save state when there is an open transaction")
	}

	state.CommitTx()
	if _, err := state.Save(); err != nil {
		t.Error("Cannot save state")
	}
}

func TestSaveRestore(t *testing.T) {
	src := NewInMem()
	src.BeginTx()
	d := "d"
	k := "k"
	v := []byte("v")
	src.Dict(d).Put(k, v)
	src.CommitTx()

	b, err := src.Save()
	if err != nil {
		t.Error(err)
	}

	dst := NewInMem()
	if err = dst.Restore(b); err != nil {
		t.Error(err)
	}

	size := 0
	dst.Dict(d).ForEach(func(k string, v []byte) { size++ })
	if size > 1 {
		t.Errorf("Dictionary has more than one entry: %v -> %v", k, v)
	}
	v2, err := dst.Dict(d).Get(k)
	if err != nil {
		t.Error("No such key in the dictionary")
	}
	if string(v2) != string(v) {
		t.Error("Invalid value in the dictionary")
	}
}
