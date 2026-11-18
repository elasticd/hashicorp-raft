// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sealedWorkersTransportV1 struct {
	*runtimeAssemblyTransportV1
	closes          atomic.Int64
	heartbeatNonNil atomic.Int64
	closeErr        error
	closeEntered    chan struct{}
	closeProceed    <-chan struct{}
}

func (source *sealedWorkersTransportV1) SetHeartbeatHandler(callback func(RPC)) {
	if callback != nil {
		source.heartbeatNonNil.Add(1)
	}
	source.runtimeAssemblyTransportV1.SetHeartbeatHandler(callback)
}

func (source *sealedWorkersTransportV1) Close() error {
	source.closes.Add(1)
	if source.closeEntered != nil {
		source.closeEntered <- struct{}{}
	}
	if source.closeProceed != nil {
		<-source.closeProceed
	}
	return errors.Join(source.InmemTransport.Close(), source.closeErr)
}

type sealedWorkersFixtureV1 struct {
	local     *sealedInitialization
	runtime   *Raft
	gate      *sealedTransportGateV1
	transport Transport
	source    *sealedWorkersTransportV1
	store     *runtimeAssemblySealedStoreV1
	snapshots *runtimeAssemblySealedSnapshotsV1
	app       *runtimeAssemblySealedApplicationV1
	cancel    context.CancelFunc
	owner     *sealedRuntimeWorkersV1
}

func newSealedWorkersFixtureV1(t *testing.T) *sealedWorkersFixtureV1 {
	t.Helper()
	original, proof := newSealedFixture()
	_, inmem := NewInmemTransport("old")
	fixture := &sealedWorkersFixtureV1{
		source:    &sealedWorkersTransportV1{runtimeAssemblyTransportV1: &runtimeAssemblyTransportV1{InmemTransport: inmem}},
		store:     &runtimeAssemblySealedStoreV1{sealedFixture: original},
		snapshots: &runtimeAssemblySealedSnapshotsV1{sealedFixture: original},
		app:       &runtimeAssemblySealedApplicationV1{sealedTestApplication: &sealedTestApplication{}},
	}
	var err error
	fixture.transport, fixture.gate, err = newSealedTransportGateV1(fixture.source)
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.LocalID = "a"
	config.HeartbeatTimeout = time.Hour
	config.ElectionTimeout = time.Hour
	config.LeaderLeaseTimeout = time.Hour
	config.SnapshotInterval = time.Hour
	fixture.runtime, err = newRaftRuntimeV1(config, appAdapter{fixture.app}, fixture.store, fixture.store, fixture.snapshots, fixture.transport)
	if err != nil {
		t.Fatal(err)
	}
	startup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	fixture.cancel = cancel
	fixture.local, err = initializeSealedRaftIntoV1(startup, fixture.runtime, proof, fixture.store, fixture.store, fixture.snapshots,
		confirmSealedFixtureApply, func(observation sealedObservation) error {
			return confirmSealedFixtureState(fixture.app.sealedTestApplication, observation)
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if fixture.owner != nil {
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			_ = fixture.owner.closeV1(ctx)
		}
		_ = fixture.gate.Close()
		fixture.runtime.Shutdown()
		joined := make(chan struct{})
		go func() { fixture.runtime.waitShutdown(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("fixture retained unjoined workers")
		}
	})
	return fixture
}

func (fixture *sealedWorkersFixtureV1) park(t *testing.T) *sealedRuntimeWorkersV1 {
	t.Helper()
	owner, err := parkSealedRuntimeWorkersV1(fixture.local, fixture.gate)
	if err != nil || owner == nil {
		t.Fatalf("park: owner=%v err=%v", owner, err)
	}
	fixture.owner = owner
	return owner
}

func sealedWorkersCloseV1(t *testing.T, owner *sealedRuntimeWorkersV1) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return owner.closeV1(ctx)
}

func sealedWorkersRequestV1(runtime *Raft) *configurationsFuture {
	request := &configurationsFuture{}
	request.init()
	request.ShutdownCh = runtime.shutdownCh
	runtime.configurationsCh <- request
	return request
}

func requireSealedWorkersRequestV1(t *testing.T, request ConfigurationFuture) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- request.Error() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker configuration request: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ordinary run worker did not answer its private configuration request")
	}
	if request.Index() != 9 || len(request.Configuration().Servers) != 1 {
		t.Fatalf("wrong configuration readback: %d %#v", request.Index(), request.Configuration())
	}
}

func requireSealedWorkersNoWorkV1(t *testing.T, fixture *sealedWorkersFixtureV1) {
	t.Helper()
	if fixture.store.writes.Load() != 0 || fixture.snapshots.creates.Load() != 0 || fixture.app.applies.Load() != 1 || fixture.app.restores.Load() != 1 || fixture.source.outboundCalls.Load() != 0 || fixture.source.heartbeatCalls.Load() != 1 || fixture.source.heartbeatNonNil.Load() != 0 {
		t.Fatalf("unexpected real work: writes=%d snapshots=%d apply=%d restore=%d outbound=%d heartbeat=%d", fixture.store.writes.Load(), fixture.snapshots.creates.Load(), fixture.app.applies.Load(), fixture.app.restores.Load(), fixture.source.outboundCalls.Load(), fixture.source.heartbeatCalls.Load())
	}
}

func TestSealedRuntimeWorkersV1ParksWithoutRealWork(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	owner := fixture.park(t)
	request := sealedWorkersRequestV1(fixture.runtime)
	response := make(chan RPCResponse, 1)
	fixture.source.InmemTransport.consumerCh <- RPC{Command: &RequestVoteRequest{Term: 5}, RespChan: response}
	select {
	case result := <-response:
		t.Fatalf("parked RPC was handled: %#v", result)
	case <-time.After(20 * time.Millisecond):
	}
	if len(fixture.runtime.configurationsCh) != 1 || len(fixture.source.InmemTransport.consumerCh) != 1 {
		t.Fatal("parked wrappers consumed work")
	}
	select {
	case err := <-request.errCh:
		t.Fatalf("parked worker answered configuration: %v", err)
	default:
	}
	requireSealedWorkersNoWorkV1(t, fixture)
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
	if fixture.source.closes.Load() != 1 || fixture.runtime.State() != Shutdown {
		t.Fatal("parked close did not retain transport and runtime custody")
	}
}

func TestSealedRuntimeWorkersV1ReleasesSameRuntimeAndDetachesStartupCancellation(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	owner := fixture.park(t)
	request := sealedWorkersRequestV1(fixture.runtime)
	runtime, err := owner.releaseV1()
	if err != nil || runtime != fixture.runtime {
		t.Fatalf("release replaced runtime: %p %v", runtime, err)
	}
	requireSealedWorkersRequestV1(t, request)
	requireSealedWorkersRequestV1(t, runtime.GetConfiguration())
	fixture.cancel()
	// A new private request must still be answered after the original startup
	// context ends; synchronous public GetConfiguration alone cannot prove this.
	requireSealedWorkersRequestV1(t, sealedWorkersRequestV1(runtime))
	if runtime.State() == Shutdown || fixture.source.closes.Load() != 0 {
		t.Fatal("startup cancellation silently closed the released lifetime")
	}
	if again, err := owner.releaseV1(); again != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("second release: %p %v", again, err)
	}
	requireSealedWorkersNoWorkV1(t, fixture)
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
}

func TestSealedRuntimeWorkersV1CancellationBeforeReleaseClosesAndJoins(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	owner := fixture.park(t)
	// Replacing the public-to-this-package slot cannot extend the original
	// captured attempt lifetime or remove its cancellation owner.
	extended, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	fixture.local.startup = extended
	fixture.cancel()
	select {
	case <-owner.closeDone:
	case <-time.After(time.Second):
		t.Fatal("original cancellation did not close parked workers")
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("cancelled attempt resurrected: %p %v", runtime, err)
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
	if fixture.source.closes.Load() != 1 || fixture.runtime.State() != Shutdown {
		t.Fatal("cancelled owner did not join exact original runtime and gate")
	}
	requireSealedWorkersNoWorkV1(t, fixture)
}

func TestSealedRuntimeWorkersV1RefusesWrongGateSparseAndSecondClaim(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	_, other, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{newSealedGateSpyV1()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	for _, candidate := range []*sealedInitialization{nil, {raft: fixture.runtime}} {
		if owner, err := parkSealedRuntimeWorkersV1(candidate, fixture.gate); owner != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
			t.Fatalf("incomplete attempt admitted: %v %v", owner, err)
		}
	}
	if owner, err := parkSealedRuntimeWorkersV1(fixture.local, other); owner != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("wrong gate admitted: %v %v", owner, err)
	}
	originalApply := fixture.runtime.applyCh
	fixture.runtime.applyCh = nil
	if owner, err := parkSealedRuntimeWorkersV1(fixture.local, fixture.gate); owner != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("sparse runtime admitted: %v %v", owner, err)
	}
	fixture.runtime.applyCh = originalApply
	owner := fixture.park(t)
	if second, err := parkSealedRuntimeWorkersV1(fixture.local, fixture.gate); second != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("second claim admitted: %v %v", second, err)
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
}

func TestSealedRuntimeWorkersV1RefusesSwappedRuntimeTerminally(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	owner := fixture.park(t)
	config := fixture.runtime.config()
	alternate, err := newRaftRuntimeV1(&config, appAdapter{fixture.app}, fixture.store, fixture.store, fixture.snapshots, fixture.transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = alternate.Shutdown().Error() })
	fixture.local.raft = alternate
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("swapped runtime admitted: %p %v", runtime, err)
	}
	fixture.local.raft = fixture.runtime
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("failed attempt revived after restoring slot: %p %v", runtime, err)
	}
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
	if fixture.runtime.State() != Shutdown || fixture.source.closes.Load() != 1 {
		t.Fatal("swapped-slot failure did not close original runtime")
	}
	requireSealedWorkersNoWorkV1(t, fixture)
}

func TestSealedRuntimeWorkersV1CloseJoinsEvenAfterShutdownWasRequested(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	owner := fixture.park(t)
	finish := make(chan struct{})
	var finishOnce sync.Once
	t.Cleanup(func() { finishOnce.Do(func() { close(finish) }) })
	entered := make(chan struct{})
	fixture.runtime.goFunc(func() { close(entered); <-finish })
	<-entered
	fixture.runtime.Shutdown() // A second Shutdown().Error() alone will not join.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := owner.closeV1(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close did not retain blocked registered worker: %v", err)
	}
	select {
	case <-owner.closeDone:
		t.Fatal("closeDone published before original registered workers joined")
	default:
	}
	finishOnce.Do(func() { close(finish) })
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
	if fixture.source.closes.Load() != 1 {
		t.Fatalf("source closes=%d", fixture.source.closes.Load())
	}
}

func TestSealedRuntimeWorkersV1ReleaseCloseRaceDoesNotResurrect(t *testing.T) {
	for attempt := 0; attempt < 16; attempt++ {
		fixture := newSealedWorkersFixtureV1(t)
		owner := fixture.park(t)
		start := make(chan struct{})
		released, closed := make(chan error, 1), make(chan error, 1)
		go func() {
			<-start
			runtime, err := owner.releaseV1()
			if runtime != nil && runtime != fixture.runtime {
				err = errors.New("replacement runtime returned")
			}
			released <- err
		}()
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			closed <- owner.closeV1(ctx)
		}()
		close(start)
		select {
		case err := <-released:
			if err != nil && !errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("racing release stuck")
		}
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("racing close stuck")
		}
		if fixture.runtime.State() != Shutdown || fixture.source.closes.Load() != 1 {
			t.Fatalf("attempt %d: runtime=%v closes=%d", attempt, fixture.runtime.State(), fixture.source.closes.Load())
		}
		if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
			t.Fatalf("attempt %d resurrected: %p %v", attempt, runtime, err)
		}
		requireSealedWorkersNoWorkV1(t, fixture)
	}
}

func TestSealedRuntimeWorkersV1RetainsStickyPhysicalCloseFailure(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	fixture.source.closeErr = errors.New("retained physical cleanup failure")
	owner := fixture.park(t)
	first := sealedWorkersCloseV1(t, owner)
	if !errors.Is(first, fixture.source.closeErr) {
		t.Fatalf("physical cleanup failure lost: %v", first)
	}
	if again := sealedWorkersCloseV1(t, owner); again != first {
		t.Fatalf("cleanup result changed: first=%v again=%v", first, again)
	}
	if fixture.source.closes.Load() != 1 || fixture.runtime.State() != Shutdown {
		t.Fatal("failed physical close abandoned original worker join")
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("failed cleanup resurrected: %p %v", runtime, err)
	}
}

func TestSealedRuntimeWorkersV1ShutdownAndJoinDoNotWaitForPhysicalClose(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	physicalProceed := make(chan struct{})
	fixture.source.closeEntered = make(chan struct{}, 1)
	fixture.source.closeProceed = physicalProceed
	var proceedOnce sync.Once
	t.Cleanup(func() { proceedOnce.Do(func() { close(physicalProceed) }) })
	owner := fixture.park(t)
	if runtime, err := owner.releaseV1(); err != nil || runtime != fixture.runtime {
		t.Fatalf("release: %p %v", runtime, err)
	}
	requireSealedWorkersRequestV1(t, sealedWorkersRequestV1(fixture.runtime))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- owner.closeV1(ctx) }()
	select {
	case <-fixture.source.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("physical Close did not enter")
	}
	select {
	case <-fixture.runtime.shutdownCh:
	default:
		t.Fatal("physical Close blocked before original shutdown was signalled")
	}
	if fixture.runtime.State() != Shutdown {
		t.Fatalf("runtime still %v while physical Close blocked", fixture.runtime.State())
	}
	joined := make(chan struct{})
	go func() { fixture.runtime.waitShutdown(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("original workers did not join while physical Close blocked")
	}
	select {
	case <-owner.closeDone:
		t.Fatal("owner forgot outstanding physical Close custody")
	default:
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("bounded caller lost cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked physical Close prevented bounded caller cancellation")
	}
	proceedOnce.Do(func() { close(physicalProceed) })
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatalf("same cleanup owner did not finish: %v", err)
	}
	if fixture.source.closes.Load() != 1 {
		t.Fatalf("physical Close repeated %d times", fixture.source.closes.Load())
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("completed cleanup resurrected: %p %v", runtime, err)
	}
}

func TestSealedRuntimeWorkersV1BeginCloseSignalsBeforePhysicalClose(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	physicalProceed := make(chan struct{})
	fixture.source.closeEntered = make(chan struct{}, 1)
	fixture.source.closeProceed = physicalProceed
	var proceedOnce sync.Once
	t.Cleanup(func() { proceedOnce.Do(func() { close(physicalProceed) }) })
	owner := fixture.park(t)
	owner.beginCloseV1()
	select {
	case <-fixture.runtime.shutdownCh:
	default:
		t.Fatal("beginClose returned before signalling runtime shutdown")
	}
	select {
	case <-owner.closeDone:
		t.Fatal("beginClose forgot blocked physical Close custody")
	default:
	}
	proceedOnce.Do(func() { close(physicalProceed) })
	if err := sealedWorkersCloseV1(t, owner); err != nil {
		t.Fatal(err)
	}
}

func TestSealedRuntimeWorkersV1PoisonDuringParkJoinsBothWatchers(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	original := fixture.local.revalidate
	fixture.local.revalidate = func() error {
		if err := original(); err != nil {
			return err
		}
		fixture.gate.failV1()
		return nil
	}
	owner := fixture.park(t)
	select {
	case <-owner.closeDone:
	case <-time.After(time.Second):
		t.Fatal("poison during park did not complete retained cleanup")
	}
	for name, done := range map[string]<-chan struct{}{
		"startup": owner.startupDone,
		"gate":    owner.gateWatchDone,
	} {
		if done == nil {
			t.Fatalf("%s watcher join channel was not initialized", name)
		}
		select {
		case <-done:
		default:
			t.Fatalf("cleanup completed before %s watcher joined", name)
		}
	}
	if fixture.runtime.State() != Shutdown || fixture.source.closes.Load() != 1 {
		t.Fatalf("poison retained runtime=%v closes=%d", fixture.runtime.State(), fixture.source.closes.Load())
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("poisoned park reopened: %p %v", runtime, err)
	}
}

func TestSealedRuntimeWorkersV1GateConsumerPoisonClosesSameRuntime(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	owner := fixture.park(t)
	// Holding the gate has not consumed this source channel. Its closure only
	// becomes observable after release opens the pump.
	close(fixture.source.InmemTransport.consumerCh)
	if runtime, err := owner.releaseV1(); err != nil && runtime != nil {
		t.Fatalf("release returned runtime with error: %p %v", runtime, err)
	}
	select {
	case <-owner.closeDone:
	case <-time.After(time.Second):
		t.Fatal("consumer poison did not join the owned runtime")
	}
	if fixture.runtime.State() != Shutdown || fixture.source.closes.Load() != 1 {
		t.Fatalf("poison retained runtime=%v closes=%d", fixture.runtime.State(), fixture.source.closes.Load())
	}
	if err := sealedWorkersCloseV1(t, owner); !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("consumer poison lost sticky gate error: %v", err)
	}
	if runtime, err := owner.releaseV1(); runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) {
		t.Fatalf("poisoned owner reopened: %p %v", runtime, err)
	}
}

func TestSealedRuntimeWorkersV1PipelinePoisonClosesSameRuntime(t *testing.T) {
	fixture := newSealedWorkersFixtureV1(t)
	owner := fixture.park(t)
	if runtime, err := owner.releaseV1(); err != nil || runtime != fixture.runtime {
		t.Fatalf("release: %p %v", runtime, err)
	}
	pipelineErr := errors.New("pipeline cleanup poison")
	pipeline := &sealedAppendPipelineV1{
		gate:   owner.transport,
		source: &sealedGatePipelineSpyV1{closeErr: pipelineErr},
	}
	if err := pipeline.Close(); err != pipelineErr {
		t.Fatalf("pipeline cleanup: %v", err)
	}
	select {
	case <-owner.closeDone:
	case <-time.After(time.Second):
		t.Fatal("pipeline poison did not join the owned runtime")
	}
	if fixture.runtime.State() != Shutdown || fixture.source.closes.Load() != 1 {
		t.Fatalf("poison retained runtime=%v closes=%d", fixture.runtime.State(), fixture.source.closes.Load())
	}
	if err := sealedWorkersCloseV1(t, owner); !errors.Is(err, pipelineErr) ||
		!errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("pipeline poison lost sticky cleanup errors: %v", err)
	}
}
