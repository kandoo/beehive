package raft

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/snap"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/wal"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/wal/walpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

// This code is from etcdserver/storage.go and keep it in sync.

type DiskStorage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap raftpb.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
}

type storage struct {
	*wal.WAL
	*snap.Snapshotter
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *storage) SaveSnap(snap raftpb.Snapshot) error {
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	err := st.WAL.SaveSnapshot(walsnap)
	if err != nil {
		return err
	}
	err = st.Snapshotter.SaveSnap(snap)
	if err != nil {
		return err
	}
	err = st.WAL.ReleaseLockTo(snap.Metadata.Index)
	if err != nil {
		return err
	}
	return nil
}

func mustMkdir(path string) {
	if err := os.MkdirAll(path, 0750); err != nil {
		glog.Fatalf("cannot create directory %v: %v", path, err)
	}
}

func exist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// OpenStorage creates or reloads the disk-backed storage in path.
func OpenStorage(node uint64, dir string, stateMachine StateMachine) (
	raftStorage *etcdraft.MemoryStorage, diskStorage DiskStorage,
	lastSnapIdx, lastEntIdx uint64, exists bool, err error) {

	// TODO(soheil): maybe store and return a custom metadata.
	glog.V(2).Infof("openning raft storage on %s", dir)

	sp := path.Join(dir, "snap")
	wp := path.Join(dir, "wal")
	exists = exist(sp) && exist(wp) && wal.Exist(wp)

	s := snap.New(sp)
	raftStorage = etcdraft.NewMemoryStorage()

	var w *wal.WAL
	if !exists {
		mustMkdir(sp)
		mustMkdir(wp)
		w, err = createWAL(node, wp)
		diskStorage = &storage{w, s}
		return
	}

	ss, err := s.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		return
	}

	if ss != nil {
		if err = stateMachine.Restore(ss.Data); err != nil {
			err = fmt.Errorf("raft: cannot restore statemachine from snapshot: %v",
				err)
			return
		}

		if err = raftStorage.ApplySnapshot(*ss); err != nil {
			err = fmt.Errorf("raft: cannot apply snapshot: %v", err)
			return
		}

		lastSnapIdx = ss.Metadata.Index
		glog.Infof("raft: recovered statemachine from snapshot at index %d",
			lastSnapIdx)
	}

	var st raftpb.HardState
	var ents []raftpb.Entry
	w, st, ents, err = readWAL(node, wp, ss)
	if err != nil {
		return
	}

	raftStorage.SetHardState(st)
	raftStorage.Append(ents)
	lastEntIdx = ents[len(ents)-1].Index

	diskStorage = &storage{w, s}
	return
}

func readWAL(node uint64, dir string, snap *raftpb.Snapshot) (w *wal.WAL,
	st raftpb.HardState, ents []raftpb.Entry, err error) {

	var walsnap walpb.Snapshot
	if snap != nil {
		walsnap.Index, walsnap.Term = snap.Metadata.Index, snap.Metadata.Term
	}

	var md []byte
	repaired := false
	for {
		if w, err = wal.Open(dir, walsnap); err != nil {
			return
		}

		if md, st, ents, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				err = fmt.Errorf("raft: cannot repair: %v", err)
				return
			}
			if !wal.Repair(dir) {
				err = fmt.Errorf("raft: repair failed: %v", err)
			} else {
				glog.Info("WAL successfully repaired")
				repaired = true
			}
			continue
		}
		break
	}

	walNode, err := strconv.ParseUint(string(md), 10, 64)
	if err != nil {
		err = fmt.Errorf("raft: cannot decode wal metadata: %v", err)
		return
	}

	if walNode != node {
		err = fmt.Errorf("raft: wal node is for %v instead of %v", walNode, node)
		return
	}

	return
}

func createWAL(node uint64, path string) (w *wal.WAL, err error) {
	glog.V(2).Infof("creating wal for %v in %s", node, path)
	w, err = wal.Create(path, []byte(strconv.FormatUint(node, 10)))
	return
}
