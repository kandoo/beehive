package raft

import "sync"

type reqCh struct {
	req Request
	ch  chan Response
}

type line struct {
	sync.Mutex

	list map[RequestID]reqCh
}

func (l *line) init() {
	l.list = make(map[RequestID]reqCh)
}

func (l *line) get(id RequestID) (req Request, ok bool) {
	l.Lock()
	defer l.Unlock()

	rc, ok := l.list[id]
	if !ok {
		return
	}
	return rc.req, true
}

func (l *line) wait(id RequestID, req Request) chan Response {
	l.Lock()
	defer l.Unlock()

	if rc, ok := l.list[id]; ok {
		return rc.ch
	}

	ch := make(chan Response, 1)
	l.list[id] = reqCh{
		req: req,
		ch:  ch,
	}
	return ch
}

func (l *line) call(r Response) {
	l.Lock()
	defer l.Unlock()

	if rc, ok := l.list[r.ID]; ok {
		rc.ch <- r
		delete(l.list, r.ID)
	}
}

func (l *line) cancel(id RequestID) {
	l.Lock()
	defer l.Unlock()

	delete(l.list, id)
}
