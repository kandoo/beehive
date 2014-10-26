package raft

import "sync"

type line struct {
	sync.Mutex

	list map[RequestID]chan Response
}

func (l *line) init() {
	l.list = make(map[RequestID]chan Response)
}

func (l *line) wait(r RequestID) chan Response {
	l.Lock()
	defer l.Unlock()

	if ch, ok := l.list[r]; ok {
		return ch
	}

	ch := make(chan Response)
	l.list[r] = ch
	return ch
}

func (l *line) call(r Response) {
	l.Lock()
	defer l.Unlock()

	if ch, ok := l.list[r.ID]; ok {
		ch <- r
	}
}

func (l *line) cancel(id RequestID) {
	l.Lock()
	defer l.Unlock()

	delete(l.list, id)
}
