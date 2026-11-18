// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"container/list"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

type localFenceStoreV1 struct {
	*InmemStore
	calls       int
	stored      []*Log
	fail        error
	beforeStore func()
}

func (s *localFenceStoreV1) StoreLogs(logs []*Log) error {
	s.calls++
	s.stored = append(s.stored, logs...)
	if s.beforeStore != nil {
		s.beforeStore()
	}
	if s.fail != nil {
		return s.fail
	}
	return s.InmemStore.StoreLogs(logs)
}

func localFenceRaftV1() (*Raft, *localFenceStoreV1) {
	store := &localFenceStoreV1{InmemStore: NewInmemStore()}
	configuration := Configuration{Servers: []Server{{Suffrage: Voter, ID: "node-a", Address: "node-a"}}}
	r := &Raft{localID: "node-a", localApplyFenceV1: true, logs: store,
		logger: hclog.NewNullLogger(), applyCh: make(chan *logFuture, 16), shutdownCh: make(chan struct{})}
	r.conf.Store(*DefaultConfig())
	r.raftState.setState(Leader)
	r.raftState.setCurrentTerm(2)
	r.setLatestConfiguration(configuration, 1)
	r.setCommittedConfiguration(configuration, 1)
	r.setCommitIndex(1)
	r.setLastLog(5, 2)
	r.leaderState.inflight = list.New()
	r.leaderState.commitment = newCommitment(make(chan struct{}, 1), configuration, 1)
	return r, store
}

func queuedLocalFenceV1(t *testing.T, r *Raft, ctx context.Context, term, index uint64, data string) *logFuture {
	t.Helper()
	future := r.ApplyWithLocalFenceV1(ctx, []byte(data), term, index)
	queued, ok := future.(*logFuture)
	if !ok {
		t.Fatalf("request did not queue: %v", future.Error())
	}
	if <-r.applyCh != queued {
		t.Fatal("different request queued")
	}
	return queued
}

func ordinaryLocalFenceV1(data string) *logFuture {
	future := &logFuture{log: Log{Type: LogCommand, Data: []byte(data)}}
	future.init()
	return future
}

func TestLocalApplyFenceV1ChecksLeaderLoop(t *testing.T) {
	conf := DefaultConfig()
	conf.EnableLocalApplyFenceV1 = true
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	cluster := MakeCluster(3, t, conf)
	defer cluster.Close()
	leader := cluster.Leader()
	if err := leader.Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	before := leader.LastIndex()
	current := leader.GetConfiguration()
	if err := current.Error(); err != nil || current.Index() == 0 {
		t.Fatalf("no configured leader: %v", err)
	}
	term, index := leader.CurrentTerm(), current.Index()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, pair := range [][2]uint64{{term + 1, index}, {term, index + 1}} {
		future := leader.ApplyWithLocalFenceV1(ctx, []byte("refused"), pair[0], pair[1])
		if err := future.Error(); !errors.Is(err, ErrLocalApplyFenceV1) || future.Index() != 0 {
			t.Fatalf("refusal got index=%d err=%v", future.Index(), err)
		}
		if leader.LastIndex() != before {
			t.Fatal("refused command appended")
		}
	}
	matching := leader.ApplyWithLocalFenceV1(ctx, []byte("matching"), term, index)
	if err := matching.Error(); err != nil || matching.Index() != before+1 {
		t.Fatalf("matching index=%d err=%v", matching.Index(), err)
	}
	ordinary := leader.Apply([]byte("ordinary"), time.Second)
	if err := ordinary.Error(); err != nil || ordinary.Index() != before+2 {
		t.Fatalf("ordinary index=%d err=%v", ordinary.Index(), err)
	}
}

func TestLocalApplyFenceV1FiltersEachBatchedCommand(t *testing.T) {
	r, store := localFenceRaftV1()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	oldTerm := queuedLocalFenceV1(t, r, ctx, 1, 1, "old term")
	oldConfig := queuedLocalFenceV1(t, r, ctx, 2, 2, "old config")
	canceledCtx, stop := context.WithCancel(ctx)
	canceled := queuedLocalFenceV1(t, r, canceledCtx, 2, 1, "canceled")
	stop()
	payload := []byte("matching")
	matching := r.ApplyWithLocalFenceV1(ctx, payload, 2, 1).(*logFuture)
	<-r.applyCh
	payload[0] = 'X'
	first, last := ordinaryLocalFenceV1("first"), ordinaryLocalFenceV1("last")
	r.dispatchLogs([]*logFuture{oldTerm, first, oldConfig, matching, canceled, last})
	if store.calls != 1 || len(store.stored) != 3 || r.LastIndex() != 8 || r.leaderState.inflight.Len() != 3 {
		t.Fatal("batch discarded unrelated work or left index gaps")
	}
	for i, future := range []*logFuture{first, matching, last} {
		if future.log.Index != uint64(6+i) || store.stored[i] != &future.log {
			t.Fatal("accepted order changed")
		}
	}
	if string(matching.log.Data) != "matching" || matching.localFence != nil {
		t.Fatal("caller bytes or fence retained")
	}
	for _, future := range []*logFuture{oldTerm, oldConfig, canceled} {
		if err := future.Error(); !errors.Is(err, ErrLocalApplyFenceV1) || future.Index() != 0 || future.localFence != nil {
			t.Fatalf("refusal index=%d err=%v", future.Index(), err)
		}
	}
}

func TestLocalApplyFenceV1RefusalBeforeStore(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Raft, *logFuture)
	}{
		{"term moved", func(r *Raft, f *logFuture) { r.raftState.setCurrentTerm(3) }},
		{"latest moved", func(r *Raft, f *logFuture) { r.configurations.latestIndex = 2 }},
		{"committed moved", func(r *Raft, f *logFuture) { r.configurations.committedIndex = 2 }},
		{"uncommitted", func(r *Raft, f *logFuture) { r.setCommitIndex(0) }},
		{"tuple changed", func(r *Raft, f *logFuture) {
			r.configurations.committed.Servers = []Server{{Suffrage: Voter, ID: "node-a", Address: "moved"}}
		}},
		{"missing tuple", func(r *Raft, f *logFuture) { r.configurations.committed.Servers = nil }},
		{"nonvoter", func(r *Raft, f *logFuture) {
			c := Configuration{Servers: []Server{{Suffrage: Nonvoter, ID: "node-a", Address: "node-a"}}}
			r.setLatestConfiguration(c, 1)
			r.setCommittedConfiguration(c, 1)
		}},
		{"local absent", func(r *Raft, f *logFuture) { r.localID = "absent" }},
		{"not leader", func(r *Raft, f *logFuture) { r.raftState.setState(Follower) }},
		{"disabled", func(r *Raft, f *logFuture) { r.localApplyFenceV1 = false }},
		{"zero term", func(r *Raft, f *logFuture) { f.localFence.term = 0; r.raftState.setCurrentTerm(0) }},
		{"zero index", func(r *Raft, f *logFuture) {
			f.localFence.configIndex = 0
			r.configurations.latestIndex = 0
			r.configurations.committedIndex = 0
		}},
		{"expired", func(r *Raft, f *logFuture) { f.localFence.deadline = time.Now().Add(-time.Second) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, store := localFenceRaftV1()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			future := queuedLocalFenceV1(t, r, ctx, 2, 1, "refused")
			test.change(r, future)
			r.dispatchLogs([]*logFuture{future})
			if err := future.Error(); !errors.Is(err, ErrLocalApplyFenceV1) || future.Index() != 0 {
				t.Fatalf("index=%d err=%v", future.Index(), err)
			}
			if store.calls != 0 || r.LastIndex() != 5 || r.leaderState.inflight.Len() != 0 {
				t.Fatal("all-refused batch touched log store or indices")
			}
		})
	}
}

type localFenceCancelContextV1 struct {
	context.Context
	beforeErr func()
}

func (c localFenceCancelContextV1) Err() error { c.beforeErr(); return c.Context.Err() }

func TestLocalApplyFenceV1ChecksLifetimePerEntry(t *testing.T) {
	r, store := localFenceRaftV1()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	laterCtx, stop := context.WithCancel(ctx)
	defer stop()
	first := queuedLocalFenceV1(t, r, ctx, 2, 1, "first")
	later := queuedLocalFenceV1(t, r, laterCtx, 2, 1, "later")
	first.localFence.ctx = localFenceCancelContextV1{Context: ctx, beforeErr: stop}
	ordinary := ordinaryLocalFenceV1("ordinary")
	r.dispatchLogs([]*logFuture{first, later, ordinary})
	if err := later.Error(); !errors.Is(err, ErrLocalApplyFenceV1) || later.Index() != 0 {
		t.Fatal("lifetime was checked only once per batch")
	}
	if store.calls != 1 || len(store.stored) != 2 || first.Index() != 6 || ordinary.Index() != 7 {
		t.Fatal("per-entry refusal changed unrelated writes")
	}
}

func TestLocalApplyFenceV1CancellationAfterAdmissionIsNotRefusal(t *testing.T) {
	r, store := localFenceRaftV1()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	future := queuedLocalFenceV1(t, r, ctx, 2, 1, "accepted")
	store.beforeStore = cancel
	r.dispatchLogs([]*logFuture{future})
	if ctx.Err() == nil || future.Index() != 6 || len(store.stored) != 1 || future.localFence != nil {
		t.Fatal("post-admission cancellation erased accepted write")
	}
	future.respond(nil)
	if err := future.Error(); err != nil {
		t.Fatalf("post-admission cancellation falsely refused: %v", err)
	}
}

func TestLocalApplyFenceV1StoreFailurePreservesRefusal(t *testing.T) {
	r, store := localFenceRaftV1()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	refused := queuedLocalFenceV1(t, r, ctx, 1, 1, "refused")
	accepted := queuedLocalFenceV1(t, r, ctx, 2, 1, "accepted")
	store.fail = errors.New("store failed")
	r.dispatchLogs([]*logFuture{refused, accepted})
	if err := refused.Error(); !errors.Is(err, ErrLocalApplyFenceV1) || refused.Index() != 0 {
		t.Fatal("store failure overwrote definite refusal")
	}
	if err := accepted.Error(); !errors.Is(err, store.fail) || accepted.Index() != 6 {
		t.Fatal("accepted store failure lost indeterminate index")
	}
}

func TestLocalApplyFenceV1RequiresBoundedOriginalAttempt(t *testing.T) {
	r, _ := localFenceRaftV1()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	canceled, stop := context.WithCancel(ctx)
	stop()
	for _, input := range []struct {
		ctx         context.Context
		data        []byte
		term, index uint64
	}{
		{nil, []byte("x"), 2, 1}, {context.Background(), []byte("x"), 2, 1}, {canceled, []byte("x"), 2, 1},
		{ctx, nil, 2, 1}, {ctx, []byte("x"), 0, 1}, {ctx, []byte("x"), 2, 0},
	} {
		future := r.ApplyWithLocalFenceV1(input.ctx, input.data, input.term, input.index)
		if err := future.Error(); !errors.Is(err, ErrLocalApplyFenceV1) || future.Index() != 0 {
			t.Fatalf("invalid attempt queued: %v", err)
		}
	}
	if len(r.applyCh) != 0 {
		t.Fatal("invalid attempt reached queue")
	}
	// An unbuffered send cannot succeed; deadline refusal is therefore definite.
	r.applyCh = make(chan *logFuture)
	short, finish := context.WithTimeout(ctx, 5*time.Millisecond)
	defer finish()
	if err := r.ApplyWithLocalFenceV1(short, []byte("x"), 2, 1).Error(); !errors.Is(err, ErrLocalApplyFenceV1) {
		t.Fatal(err)
	}
}

type localFenceHeartbeatRecorderV1 struct {
	*InmemTransport
	called bool
	fast   bool
}

// SetHeartbeatHandler records the constructor's chosen transport mode.
func (recorder *localFenceHeartbeatRecorderV1) SetHeartbeatHandler(callback func(RPC)) {
	recorder.called = true
	recorder.fast = callback != nil
}

func TestLocalApplyFenceV1ConstructorOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		address, mem := NewInmemTransport("node-a")
		transport := &localFenceHeartbeatRecorderV1{InmemTransport: mem}
		conf := DefaultConfig()
		conf.LocalID = ServerID(address)
		conf.skipStartup = true
		conf.EnableLocalApplyFenceV1 = enabled
		r, err := NewRaft(conf, &MockFSM{}, NewInmemStore(), NewInmemStore(), NewInmemSnapshotStore(), transport)
		if err != nil {
			t.Fatal(err)
		}
		if !transport.called || transport.fast == enabled || r.localApplyFenceV1 != enabled {
			t.Fatal("heartbeat mode does not establish opt-in")
		}
		conf.EnableLocalApplyFenceV1 = !enabled
		if r.localApplyFenceV1 != enabled {
			t.Fatal("caller changed immutable constructor choice")
		}
		if err := r.Shutdown().Error(); err != nil {
			t.Fatal(err)
		}
		if !enabled {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			future := r.ApplyWithLocalFenceV1(ctx, []byte("held"), 1, 1)
			cancel()
			if err := future.Error(); !errors.Is(err, ErrLocalApplyFenceV1) || future.Index() != 0 {
				t.Fatal("default constructor enabled fenced API")
			}
		}
	}
}
