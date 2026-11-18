// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func newSealedRuntimeV1Fixture(t *testing.T) (*SealedRuntimeV1,
	*runtimeAssemblySealedStoreV1, *runtimeAssemblySealedSnapshotsV1,
	*sealedWorkersTransportV1, *sealedRuntimeConstructorApplicationV1,
	context.CancelFunc,
) {
	t.Helper()
	checkpoint, store, snapshots, source, app, conf :=
		newSealedRuntimeConstructorInputsV1(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	runtime, err := NewSealedRuntimeV1(ctx, conf, checkpoint, store, store,
		snapshots, source, app, confirmSealedFixtureApply,
		func(got SealedReadbackV1) error {
			return confirmSealedFixtureState(app.sealedTestApplication, got)
		})
	if err != nil || runtime == nil {
		cancel()
		t.Fatalf("NewSealedRuntimeV1: runtime=%p err=%v", runtime, err)
	}
	return runtime, store, snapshots, source, app, cancel
}

func sealedRuntimeV1Close(t *testing.T, runtime *SealedRuntimeV1) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return runtime.CloseV1(ctx)
}

func TestSealedRuntimeV1RefusesNilZeroAndPreAcquisition(t *testing.T) {
	var nilRuntime *SealedRuntimeV1
	var zero SealedRuntimeV1
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, runtime := range []*SealedRuntimeV1{nilRuntime, &zero} {
		if got, err := runtime.ReleaseV1(); got != nil ||
			!errors.Is(err, errSealedRuntimeWorkersV1) {
			t.Fatalf("release refusal: %p %v", got, err)
		}
		if err := runtime.CloseV1(ctx); !errors.Is(err, errSealedRuntimeWorkersV1) {
			t.Fatalf("close refusal: %v", err)
		}
	}
	checkpoint, store, snapshots, source, app, conf :=
		newSealedRuntimeConstructorInputsV1(t)
	unsupported := sealedBatchTestFSM{appAdapter{app}}
	runtime, err := NewSealedRuntimeV1(ctx, conf, checkpoint, store, store,
		snapshots, source, unsupported, confirmSealedFixtureApply,
		func(SealedReadbackV1) error { return nil })
	if runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) ||
		app.restores.Load() != 0 || source.closes.Load() != 0 {
		t.Fatalf("pre-acquisition rejection: runtime=%p err=%v", runtime, err)
	}
}

func TestSealedRuntimeV1RetainsPartialOwnerAndCleanup(t *testing.T) {
	for _, stage := range []string{"assembly", "replay"} {
		t.Run(stage, func(t *testing.T) {
			checkpoint, store, snapshots, source, app, conf :=
				newSealedRuntimeConstructorInputsV1(t)
			confirm := confirmSealedFixtureApply
			if stage == "assembly" {
				conf.HeartbeatTimeout = 0
			} else {
				confirm = func(Log, interface{}) error {
					return errors.New("replay confirmation failed")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			runtime, err := NewSealedRuntimeV1(ctx, conf, checkpoint, store,
				store, snapshots, source, app, confirm,
				func(got SealedReadbackV1) error {
					return confirmSealedFixtureState(app.sealedTestApplication, got)
				})
			if runtime == nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatalf("partial owner lost: %p %v", runtime, err)
			}
			if err := sealedRuntimeV1Close(t, runtime); err != nil {
				t.Fatal(err)
			}
			if source.closes.Load() != 1 || store.writes.Load() != 0 ||
				source.outboundCalls.Load() != 0 {
				t.Fatal("partial owner did not retain same cleanup")
			}
		})
	}
}

func TestSealedRuntimeV1AliasesReleaseAndCloseOnce(t *testing.T) {
	runtime, _, _, source, _, cancel := newSealedRuntimeV1Fixture(t)
	defer cancel()
	alias := *runtime
	got, err := runtime.ReleaseV1()
	if err != nil || got == nil || got != runtime.owner.runtime {
		t.Fatalf("release: %p %v", got, err)
	}
	if again, err := alias.ReleaseV1(); again != nil ||
		!errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("alias released twice: %p %v", again, err)
	}
	source.closeErr = errors.New("sticky physical close")
	if first := sealedRuntimeV1Close(t, &alias); !errors.Is(first, source.closeErr) {
		t.Fatalf("first close: %v", first)
	} else if again := sealedRuntimeV1Close(t, runtime); again != first {
		t.Fatalf("close result changed: first=%v again=%v", first, again)
	}
	if source.closes.Load() != 1 {
		t.Fatalf("physical closes=%d", source.closes.Load())
	}
}

func TestSealedRuntimeV1PreservesHeldRuntimeAndServingCustody(t *testing.T) {
	runtime, store, snapshots, source, app, cancel := newSealedRuntimeV1Fixture(t)
	defer cancel()
	expected, _ := newSealedFixture()
	if runtime.owner.runtime.fsm != app || app.restores.Load() != 1 ||
		app.applies.Load() != 1 || runtime.owner.runtime.LastIndex() != 11 ||
		!reflect.DeepEqual(store.sealedFixture, expected) {
		t.Fatal("constructor did not retain exact runtime, replay, and suffix")
	}
	response := make(chan RPCResponse, 1)
	source.InmemTransport.consumerCh <- RPC{Command: &RequestVoteRequest{
		Term: 5}, RespChan: response}
	if len(source.InmemTransport.consumerCh) != 1 ||
		source.outboundCalls.Load() != 0 || source.heartbeatNonNil.Load() != 0 {
		t.Fatal("held runtime emitted transport work")
	}
	got, err := runtime.ReleaseV1()
	if err != nil || got != runtime.owner.runtime {
		t.Fatalf("release changed runtime: %p %v", got, err)
	}
	got.Shutdown()
	if err := sealedRuntimeV1Close(t, runtime); err != nil {
		t.Fatal(err)
	}
	if source.closes.Load() != 1 || !reflect.DeepEqual(store.sealedFixture, expected) ||
		snapshots.creates.Load() != 0 {
		t.Fatal("external shutdown bypassed wrapper custody")
	}
}

func TestSealedRuntimeV1CancellationTimeoutAndJoin(t *testing.T) {
	runtime, _, _, source, _, cancel := newSealedRuntimeV1Fixture(t)
	defer cancel()
	blocked := make(chan struct{})
	source.closeEntered = make(chan struct{}, 1)
	source.closeProceed = blocked
	var once sync.Once
	defer once.Do(func() { close(blocked) })
	cancel()
	select {
	case <-source.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("startup cancellation did not start retained cleanup")
	}
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if err := runtime.CloseV1(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded wait: %v", err)
	}
	once.Do(func() { close(blocked) })
	if err := sealedRuntimeV1Close(t, runtime); err != nil {
		t.Fatal(err)
	}
	if runtime.owner.runtime.State() != Shutdown || source.closes.Load() != 1 {
		t.Fatal("cancelled owner did not join exact lifetime")
	}
}
