package state

import "testing"

func testTx(t *testing.T, parent State, tx *Transactional, open bool) {
	d := "d"
	k1 := "testkey1"
	k2 := "testkey2"
	v := "testvalue"
	if !open {
		if err := tx.BeginTx(); err != nil {
			t.Errorf("error in begin tx: %v", err)
		}
		if err := tx.Dict(d).Put(k1, v); err != nil {
			t.Errorf("error in put: %v", err)
		}
	}
	if _, err := parent.Dict(d).Get(k1); err == nil {
		t.Error("value is in the dictionary before commit")
	}
	if err := tx.CommitTx(); err != nil {
		t.Errorf("error in commit tx: %v", err)
	}
	if _, err := parent.Dict(d).Get(k1); err != nil {
		t.Error("value is not in the dictionary after commit")
	}
	if err := tx.BeginTx(); err != nil {
		t.Errorf("error in begin tx: %v", err)
	}
	if err := tx.Dict(d).Put(k2, v); err != nil {
		t.Errorf("error in put: %v", err)
	}
	if err := tx.AbortTx(); err != nil {
		t.Errorf("error in abort tx: %v", err)
	}
	if _, err := parent.Dict(d).Get(k2); err == nil {
		t.Error("value is in the dictionary after abort")
	}

}

func TestTxCommit(t *testing.T) {
	inm := NewInMem()
	tx := NewTransactional(inm)
	testTx(t, inm, tx, false)
}

func TestLayeredTxCommit(t *testing.T) {
	inm := NewInMem()
	tx1 := NewTransactional(inm)
	tx1.BeginTx()
	tx2 := NewTransactional(tx1)
	testTx(t, tx1, tx2, false)
	testTx(t, inm, tx1, true)
}

func BenchmarkTransactions(b *testing.B) {
	inm := NewInMem()
	tx := NewTransactional(inm)
	d := "d"
	k := "k"
	v := "v"
	for i := 0; i < b.N; i++ {
		tx.BeginTx()
		tx.Dict(d).Put(k, v)
		tx.CommitTx()
	}
}
