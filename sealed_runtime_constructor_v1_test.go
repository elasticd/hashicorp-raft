// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type sealedRuntimeConstructorApplicationV1 struct {
	*runtimeAssemblySealedApplicationV1
	snapshotErr error
}

func (app *sealedRuntimeConstructorApplicationV1) Snapshot() (
	FSMSnapshot, error,
) {
	return nil, app.snapshotErr
}

func newSealedRuntimeConstructorInputsV1(t *testing.T) (
	SealedCheckpointV1, *runtimeAssemblySealedStoreV1,
	*runtimeAssemblySealedSnapshotsV1, *sealedWorkersTransportV1,
	*sealedRuntimeConstructorApplicationV1, *Config,
) {
	t.Helper()
	fixture, proof := newSealedFixture()
	store := &runtimeAssemblySealedStoreV1{sealedFixture: fixture}
	snapshots := &runtimeAssemblySealedSnapshotsV1{sealedFixture: fixture}
	_, inmem := NewInmemTransport("new")
	transport := &sealedWorkersTransportV1{
		runtimeAssemblyTransportV1: &runtimeAssemblyTransportV1{
			InmemTransport: inmem,
		},
	}
	sealedApp := &runtimeAssemblySealedApplicationV1{
		sealedTestApplication: &sealedTestApplication{},
	}
	app := &sealedRuntimeConstructorApplicationV1{
		runtimeAssemblySealedApplicationV1: sealedApp,
		snapshotErr:                        errors.New("outer snapshot guard"),
	}
	conf := DefaultConfig()
	conf.LocalID = "a"
	conf.HeartbeatTimeout = time.Hour
	conf.ElectionTimeout = time.Hour
	conf.LeaderLeaseTimeout = time.Hour
	conf.SnapshotInterval = time.Hour
	checkpoint := SealedCheckpointV1{
		Snapshot: proof.snapshot, BodySHA256: proof.bodyDigest,
		TailIndex: proof.tailIndex, TailTerm: proof.tailTerm,
		TailSHA256: proof.tailDigest,
	}
	return checkpoint, store, snapshots, transport, app, conf
}

func TestNewSealedRuntimeWorkersV1ParksSameRuntime(t *testing.T) {
	checkpoint, store, snapshots, source, app, conf :=
		newSealedRuntimeConstructorInputsV1(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store,
		store, snapshots, source, app, confirmSealedFixtureApply,
		func(got SealedReadbackV1) error {
			return confirmSealedFixtureState(app.sealedTestApplication, got)
		})
	if err != nil || owner == nil || owner.runtime == nil ||
		owner.runtime.logs != store || owner.runtime.stable != store ||
		owner.runtime.snapshots != snapshots || owner.source != source ||
		owner.app != app {
		t.Fatalf("constructor did not retain original owner: %p %v", owner,
			err)
	}
	if owner.runtime.fsm != app {
		t.Fatalf("constructor replaced guarded FSM: got %T", owner.runtime.fsm)
	}
	if _, err := owner.runtime.fsm.Snapshot(); err != app.snapshotErr {
		t.Fatalf("runtime lost guarded Snapshot result: %v", err)
	}
	if owner.phase != sealedWorkersHeldV1 || app.restores.Load() != 1 ||
		app.applies.Load() != 1 || source.outboundCalls.Load() != 0 ||
		source.heartbeatNonNil.Load() != 0 {
		t.Fatal("constructor performed serving work before release")
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
}

func TestNewSealedRuntimeWorkersV1RetainsPostGateFailure(t *testing.T) {
	checkpoint, store, snapshots, source, app, conf :=
		newSealedRuntimeConstructorInputsV1(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	confirmation := errors.New("confirmation failed after gate")
	owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store,
		store, snapshots, source, app, confirmSealedFixtureApply,
		func(SealedReadbackV1) error { return confirmation })
	if owner == nil || !errors.Is(err, confirmation) ||
		owner.source != source || owner.logs != store ||
		owner.stable != store || owner.snapshots != snapshots ||
		owner.app != app {
		t.Fatalf("post-gate failure lost retained custody: %p %v", owner, err)
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
	if source.closes.Load() != 1 {
		t.Fatalf("physical close count: %d", source.closes.Load())
	}
}

func TestNewSealedRuntimeWorkersV1RefusesUnboundIdentity(t *testing.T) {
	checkpoint, store, snapshots, source, app, conf :=
		newSealedRuntimeConstructorInputsV1(t)
	conf.LocalID = "not-a-voter"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store,
		store, snapshots, source, app, confirmSealedFixtureApply,
		func(got SealedReadbackV1) error {
			return confirmSealedFixtureState(app.sealedTestApplication, got)
		})
	if owner == nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("unbound identity admitted: %p %v", owner, err)
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
}

func cleanupSealedRuntimeConstructorOwnerV1(t *testing.T, owner *sealedRuntimeWorkersV1) {
	t.Helper()
	if owner != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = owner.closeV1(ctx)
		})
	}
}

type sealedRuntimeConstructorCallbackTransportV1 struct {
	*sealedWorkersTransportV1
	once     sync.Once
	callback func()
}

func (source *sealedRuntimeConstructorCallbackTransportV1) LocalAddr() ServerAddress {
	source.once.Do(source.callback)
	return source.sealedWorkersTransportV1.LocalAddr()
}

func TestNewSealedRuntimeWorkersV1RejectsUnsupportedFSMBeforeRestore(t *testing.T) {
	for _, capability := range []string{"batch", "configuration"} {
		t.Run(capability, func(t *testing.T) {
			checkpoint, store, snapshots, source, app, conf := newSealedRuntimeConstructorInputsV1(t)
			var unsupported FSM = sealedBatchTestFSM{appAdapter{app}}
			if capability == "configuration" {
				unsupported = sealedConfigurationTestFSM{appAdapter{app}}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store, store, snapshots, source, unsupported, confirmSealedFixtureApply, func(SealedReadbackV1) error { return nil })
			cleanupSealedRuntimeConstructorOwnerV1(t, owner)
			if owner != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatalf("unsupported FSM acquired constructor ownership: %p %v", owner, err)
			}
			if app.restores.Load() != 0 || app.applies.Load() != 0 || source.closes.Load() != 0 || source.heartbeatCalls.Load() != 0 {
				t.Fatal("unsupported FSM reached restore or transport acquisition")
			}
		})
	}
}

func TestNewSealedRuntimeWorkersV1CopiesMetadataBeforeSourceCallback(t *testing.T) {
	checkpoint, store, snapshots, source, app, conf := newSealedRuntimeConstructorInputsV1(t)
	// Detach caller metadata from the real retained snapshot before mutating
	// it. Mutating a source would be genuine proof drift, not a copy test.
	checkpoint.Snapshot.Configuration = checkpoint.Snapshot.Configuration.Clone()
	store.meta.Peers = []byte("original checkpoint peers")
	checkpoint.Snapshot.Peers = bytes.Clone(store.meta.Peers)
	callback := &sealedRuntimeConstructorCallbackTransportV1{sealedWorkersTransportV1: source}
	callback.callback = func() {
		conf.LocalID = "foreign"
		conf.HeartbeatTimeout = 0
		checkpoint.Snapshot.Configuration.Servers[0].ID = "foreign"
		checkpoint.Snapshot.Peers[0] = 'x'
		checkpoint.TailIndex = 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store, store, snapshots, callback, app, confirmSealedFixtureApply, func(got SealedReadbackV1) error {
		return confirmSealedFixtureState(app.sealedTestApplication, got)
	})
	cleanupSealedRuntimeConstructorOwnerV1(t, owner)
	if err != nil || owner == nil {
		t.Fatalf("caller metadata mutation altered detached attempt: %p %v", owner, err)
	}
	if owner.runtime.config().LocalID != "a" || owner.runtime.config().HeartbeatTimeout != time.Hour || owner.runtime.AppliedIndex() != 10 || owner.source != callback || owner.runtime.fsm != app {
		t.Fatal("constructor did not retain copied metadata and exact guarded FSM")
	}
	if store.meta.Configuration.Servers[0].ID != "a" || string(store.meta.Peers) != "original checkpoint peers" {
		t.Fatal("test mutated original proof sources")
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
}

func TestNewSealedRuntimeWorkersV1RejectsCorrectIDAtWrongAddress(t *testing.T) {
	checkpoint, store, snapshots, source, app, conf := newSealedRuntimeConstructorInputsV1(t)
	source.InmemTransport.localAddr = "old"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store, store, snapshots, source, app, confirmSealedFixtureApply, func(got SealedReadbackV1) error { return confirmSealedFixtureState(app.sealedTestApplication, got) })
	cleanupSealedRuntimeConstructorOwnerV1(t, owner)
	if owner == nil || !errors.Is(err, errSealedRuntimeWorkersV1) || owner.runtime == nil || owner.local == nil {
		t.Fatalf("same voter ID at wrong address admitted or custody lost: %p %v", owner, err)
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
	if source.closes.Load() != 1 || source.outboundCalls.Load() != 0 {
		t.Fatal("wrong-address refusal lost cleanup or emitted RPC")
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("wrong-address attempt reused: %p %v", runtime, err)
	}
}

func TestNewSealedRuntimeWorkersV1RetainsAssemblyAndReplayFailureStages(t *testing.T) {
	for _, stage := range []string{"assembly", "replay"} {
		t.Run(stage, func(t *testing.T) {
			checkpoint, store, snapshots, source, app, conf := newSealedRuntimeConstructorInputsV1(t)
			replayErr := errors.New("original replay confirmation failure")
			confirm := confirmSealedFixtureApply
			if stage == "assembly" {
				conf.HeartbeatTimeout = 0
			} else {
				confirm = func(Log, interface{}) error { return replayErr }
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store, store, snapshots, source, app, confirm, func(got SealedReadbackV1) error { return confirmSealedFixtureState(app.sealedTestApplication, got) })
			cleanupSealedRuntimeConstructorOwnerV1(t, owner)
			if owner == nil || !errors.Is(err, errSealedRuntimeWorkersV1) || owner.source != source || owner.app != app || owner.logs != store || owner.stable != store || owner.snapshots != snapshots || owner.local != nil {
				t.Fatalf("%s failure lost original custody: %p %v", stage, owner, err)
			}
			if stage == "assembly" {
				if owner.runtime != nil || app.restores.Load() != 0 || app.applies.Load() != 0 {
					t.Fatal("failed assembly retained a fabricated runtime or replayed FSM")
				}
			} else if owner.runtime == nil || !errors.Is(err, replayErr) || app.restores.Load() != 1 || app.applies.Load() != 1 {
				t.Fatalf("replay failure lost stage or original cause: runtime=%p err=%v", owner.runtime, err)
			}
			if err := sealedWorkersCloseV1(t, owner); err != nil {
				t.Fatal(err)
			}
			if source.closes.Load() != 1 || store.writes.Load() != 0 || source.outboundCalls.Load() != 0 {
				t.Fatal("failed stage did not close once without writes or RPC")
			}
			if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatalf("%s failure reused: %p %v", stage, runtime, err)
			}
		})
	}
}

func TestNewSealedRuntimeWorkersV1ConstructionCancellationRetainsStage(t *testing.T) {
	for _, stage := range []string{"before-gate", "source-callback", "replay-callback"} {
		t.Run(stage, func(t *testing.T) {
			checkpoint, store, snapshots, source, app, conf := newSealedRuntimeConstructorInputsV1(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var transport Transport = source
			confirm := confirmSealedFixtureApply
			switch stage {
			case "before-gate":
				cancel()
			case "source-callback":
				transport = &sealedRuntimeConstructorCallbackTransportV1{sealedWorkersTransportV1: source, callback: cancel}
			case "replay-callback":
				confirm = func(log Log, result interface{}) error { cancel(); return confirmSealedFixtureApply(log, result) }
			}
			owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store, store, snapshots, transport, app, confirm, func(got SealedReadbackV1) error { return confirmSealedFixtureState(app.sealedTestApplication, got) })
			cleanupSealedRuntimeConstructorOwnerV1(t, owner)
			if !errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatalf("cancelled construction admitted: %p %v", owner, err)
			}
			if stage == "before-gate" {
				if owner != nil || app.restores.Load() != 0 || source.closes.Load() != 0 {
					t.Fatal("pre-cancelled construction acquired a gate or replayed")
				}
				return
			}
			if owner == nil || owner.runtime == nil || owner.local != nil || owner.source != transport {
				t.Fatalf("post-gate cancellation lost partial owner: %p %v", owner, err)
			}
			wantApplies := int64(0)
			if stage == "replay-callback" {
				wantApplies = 1
			}
			if app.applies.Load() != wantApplies {
				t.Fatalf("cancelled %s applied %d entries", stage, app.applies.Load())
			}
			if app.restores.Load() != wantApplies {
				t.Fatalf("cancelled %s restored %d times", stage, app.restores.Load())
			}
			if stage == "replay-callback" && !errors.Is(err, context.Canceled) {
				t.Fatalf("replay cancellation cause lost: %v", err)
			}
			if err := sealedWorkersCloseV1(t, owner); err != nil {
				t.Fatal(err)
			}
			if source.closes.Load() != 1 || store.writes.Load() != 0 || source.outboundCalls.Load() != 0 {
				t.Fatal("cancelled construction lost once-only cleanup or performed serving writes")
			}
			if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatalf("cancelled construction reused: %p %v", runtime, err)
			}
		})
	}
}

func TestNewSealedRuntimeWorkersV1FailedAssemblyRetainsCloseTimeoutAndError(t *testing.T) {
	checkpoint, store, snapshots, source, app, conf := newSealedRuntimeConstructorInputsV1(t)
	conf.HeartbeatTimeout = 0
	source.closeErr = errors.New("retained failed-assembly physical close")
	source.closeEntered = make(chan struct{}, 1)
	proceed := make(chan struct{})
	source.closeProceed = proceed
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store, store, snapshots, source, app, confirmSealedFixtureApply, func(SealedReadbackV1) error { return nil })
	cleanupSealedRuntimeConstructorOwnerV1(t, owner)
	var proceedOnce sync.Once
	t.Cleanup(func() { proceedOnce.Do(func() { close(proceed) }) })
	if owner == nil || owner.runtime != nil || owner.local != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("failed assembly did not retain exact stage: %p %v", owner, err)
	}
	select {
	case <-source.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("physical close never entered")
	}
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if err := owner.closeV1(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked physical close lost bounded deadline: %v", err)
	}
	select {
	case <-owner.closeDone:
		t.Fatal("cleanup custody completed while physical close remained blocked")
	default:
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("timed-out partial owner reused: %p %v", runtime, err)
	}
	proceedOnce.Do(func() { close(proceed) })
	first := sealedWorkersCloseV1(t, owner)
	if !errors.Is(first, source.closeErr) {
		t.Fatalf("physical close error lost after deadline: %v", first)
	}
	if again := sealedWorkersCloseV1(t, owner); again != first || source.closes.Load() != 1 {
		t.Fatalf("cleanup error not sticky/once: first=%v again=%v calls=%d", first, again, source.closes.Load())
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("failed physical cleanup reused: %p %v", runtime, err)
	}
}

func TestNewSealedRuntimeWorkersV1PreservesExactTailSuffixAndHeldRPC(t *testing.T) {
	checkpoint, store, snapshots, source, app, conf := newSealedRuntimeConstructorInputsV1(t)
	expected, _ := newSealedFixture()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	owner, err := newSealedRuntimeWorkersV1(ctx, conf, checkpoint, store, store, snapshots, source, app, confirmSealedFixtureApply, func(got SealedReadbackV1) error { return confirmSealedFixtureState(app.sealedTestApplication, got) })
	cleanupSealedRuntimeConstructorOwnerV1(t, owner)
	if err != nil || owner == nil || !owner.transport.heldV1() || owner.runtime.fsm != app {
		t.Fatalf("exact guarded runtime was not parked: %p %v", owner, err)
	}
	runtime := owner.runtime
	if runtime.CurrentTerm() != 6 || runtime.CommitIndex() != 10 || runtime.AppliedIndex() != 10 || runtime.LastIndex() != 11 || !reflect.DeepEqual(app.calls, []uint64{10}) || string(app.data) != "old tail" {
		t.Fatal("constructor replayed outside exact proved tail")
	}
	if !reflect.DeepEqual(store.sealedFixture, expected) || store.writes.Load() != 0 || snapshots.creates.Load() != 0 {
		t.Fatal("constructor modified original source or retained suffix")
	}
	response := make(chan RPCResponse, 1)
	source.InmemTransport.consumerCh <- RPC{Command: &RequestVoteRequest{Term: 5}, RespChan: response}
	if len(source.InmemTransport.consumerCh) != 1 {
		t.Fatal("held constructor drained source RPC")
	}
	select {
	case got := <-response:
		t.Fatalf("held constructor answered RPC: %#v", got)
	default:
	}
	if source.outboundCalls.Load() != 0 || source.heartbeatNonNil.Load() != 0 {
		t.Fatal("constructor started peer traffic before release")
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.sealedFixture, expected) || source.closes.Load() != 1 {
		t.Fatal("parked cleanup changed source or repeated physical close")
	}
}
