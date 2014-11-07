package composition

import (
	"errors"
	"strconv"
	"testing"

	bh "github.com/kandoo/beehive"
)

type mockHandler struct {
	rcvFunc  bh.RcvFunc
	rcvCalls int
	mapFunc  bh.MapFunc
	mapCalls int
}

func (h *mockHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	h.rcvCalls++
	if h.rcvFunc == nil {
		return nil
	}
	return h.rcvFunc(msg, ctx)
}

func (h *mockHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	h.mapCalls++
	if h.mapFunc == nil {
		return nil
	}
	return h.mapFunc(msg, ctx)
}

func localMap(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return ctx.LocalMappedCells()
}

func nilMap(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return nil
}

func errRcv(msg bh.Msg, ctx bh.RcvContext) error {
	return errors.New("error in rcv")
}

func panicMap(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	panic("can only panic")
}

func panicRcv(msg bh.Msg, ctx bh.RcvContext) error {
	panic("can only panic")
}

func testComposition(t *testing.T, handlers []*mockHandler,
	composed *ComposedHandler, expRcvCalls []int, expMapCalls []int,
	expRcvError bool, expMappedCell bh.MappedCells, txAborted bool) {

	ctx := newMockContext()
	cells := composed.Map(nil, ctx)
	for i := range cells {
		if cells[i] != expMappedCell[i] {
			t.Errorf("invalid mapped cell: actual=%v want=%v", cells[i],
				expMappedCell[i])
		}
	}

	if len(cells) != len(expMappedCell) {
		t.Errorf("invalid mapped cells: actual=%v want=%v", cells, expMappedCell)
	}

	err := composed.Rcv(nil, ctx)
	if err != nil && !expRcvError {
		t.Errorf("rcv returns an error when it should not: %v", err)
	}
	if err == nil && expRcvError {
		t.Errorf("rcv returns no error when it should: %v", err)
	}

	for i, h := range handlers {
		if expRcvCalls[i] != h.rcvCalls {
			t.Errorf("unexpected number of calls to rcv: actual=%v want=%v",
				h.rcvCalls, expRcvCalls[i])
		}
		if expMapCalls[i] != h.mapCalls {
			t.Errorf("unexpected number of calls to map: actual=%v want=%v",
				h.mapCalls, expMapCalls[i])
		}
	}

	if ctx.txAborted != txAborted {
		t.Errorf("invalid transcation status: actual=%v want=%v",
			ctx.txAborted, txAborted)
	}
}

func resetCalls(handlers []*mockHandler) {
	for _, h := range handlers {
		h.rcvCalls = 0
		h.mapCalls = 0
	}
}

func TestEverything(t *testing.T) {
	ctx := newMockContext()

	handlers := []*mockHandler{
		&mockHandler{
			mapFunc: localMap,
		},
		&mockHandler{
			mapFunc: localMap,
			rcvFunc: func(msg bh.Msg, ctx bh.RcvContext) error { return nil },
		},
		&mockHandler{
			mapFunc: localMap,
		},
	}

	localCell := ctx.LocalMappedCells()[0]
	cells := bh.MappedCells{}
	for i := range handlers {
		cell := localCell
		cell.Dict = strconv.Itoa(i) + cell.Dict
		cells = append(cells, cell)
	}

	composed := All(handlers[0], handlers[1], handlers[2]).(*ComposedHandler)
	testComposition(t, handlers, composed, []int{1, 1, 1}, []int{1, 1, 1}, false,
		cells, false)

	resetCalls(handlers)
	composed = Any(handlers[0], handlers[1], handlers[2]).(*ComposedHandler)
	testComposition(t, handlers, composed, []int{1, 0, 0}, []int{1, 1, 1}, false,
		cells, false)

	resetCalls(handlers)
	handlers[1].mapFunc = nilMap
	handlers[1].rcvFunc = errRcv
	composed = All(handlers[0], handlers[1], handlers[2]).(*ComposedHandler)
	testComposition(t, handlers, composed, []int{1, 1, 0}, []int{1, 1, 0}, false,
		bh.MappedCells{}, false)

	resetCalls(handlers)
	handlers[1].mapFunc = panicMap
	handlers[1].rcvFunc = panicRcv
	composed = All(handlers[0], handlers[1], handlers[2]).(*ComposedHandler)
	testComposition(t, handlers, composed, []int{1, 1, 0}, []int{1, 1, 0}, false,
		bh.MappedCells{}, false)

	resetCalls(handlers)
	handlers[0].mapFunc = localMap
	handlers[0].rcvFunc = errRcv
	handlers[1].mapFunc = localMap
	handlers[1].rcvFunc = panicRcv
	composed = Any(handlers[0], handlers[1], handlers[2]).(*ComposedHandler)
	testComposition(t, handlers, composed, []int{1, 1, 1}, []int{1, 1, 1}, false,
		cells, false)

	resetCalls(handlers)
	handlers[1].mapFunc = localMap
	handlers[1].rcvFunc = nil
	composed = Any(handlers[0], handlers[1], handlers[2]).(*ComposedHandler)
	testComposition(t, handlers, composed, []int{1, 1, 0}, []int{1, 1, 1}, false,
		cells, false)
}
