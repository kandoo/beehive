package bh

import (
	"bytes"
	"testing"
)

func testInMemTx(t *testing.T, abort bool) {
	state := inMemoryState{
		Dicts: make(map[DictName]*inMemDict),
	}

	err := state.BeginTx()
	if err != nil {
		t.Errorf("Error in tx begin: %v", err)
	}

	n1 := DictName("TestDict1")
	d1 := state.Dict(n1)

	keys := []Key{Key("k1"), Key("k2")}
	vals := []Value{Value("v1"), Value("v2")}

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
		state.AbortTx()
	} else {
		state.CommitTx()
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
}

func TestCommit(t *testing.T) {
	testInMemTx(t, false)
}

func TestAbort(t *testing.T) {
	testInMemTx(t, true)
}
