// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"sync"
)

var errSealedRuntimeWorkersV1 = errors.New("sealed runtime worker transition refused")

type sealedWorkerPhaseV1 uint8

const (
	sealedWorkersHeldV1 sealedWorkerPhaseV1 = iota
	sealedWorkersReleasingV1
	sealedWorkersReleasedV1
	sealedWorkersClosingV1
	sealedWorkersClosedV1
)

// sealedRuntimeWorkersV1 owns one private worker attempt. Its release is only
// a library transition: a production caller must also retain and validate the
// actual authenticated physical-publication owner before invoking it.
type sealedRuntimeWorkersV1 struct {
	local         *sealedInitialization
	runtime       *Raft
	startup       context.Context
	revalidate    func() error
	transport     *sealedTransportGateV1
	logs          LogStore
	stable        StableStore
	snapshots     SnapshotStore
	app           FSM
	source        Transport
	mu            sync.Mutex
	phase         sealedWorkerPhaseV1
	start         chan struct{}
	startupStop   chan struct{}
	startupDone   chan struct{}
	gateWatchDone chan struct{}
	stopOnce      sync.Once
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
}

// parkSealedRuntimeWorkersV1 registers exactly the ordinary three workers but
// parks their real bodies behind one owned gate. It cannot park the old sparse
// readback-only object, borrow a different transport, or claim a node twice.
func parkSealedRuntimeWorkersV1(local *sealedInitialization, transport *sealedTransportGateV1) (*sealedRuntimeWorkersV1, error) {
	owner := newSealedRuntimeWorkerOwnerV1(transport)
	return parkSealedRuntimeWorkersIntoV1(owner, local, transport)
}

// newSealedRuntimeWorkerOwnerV1 allocates retained cleanup custody as soon as
// its original transport gate has been acquired. Callers fill the original
// inputs before construction can fail at a later stage.
func newSealedRuntimeWorkerOwnerV1(
	transport *sealedTransportGateV1,
) *sealedRuntimeWorkersV1 {
	return &sealedRuntimeWorkersV1{
		transport: transport,
		phase:     sealedWorkersHeldV1,
		start:     make(chan struct{}),
		closeDone: make(chan struct{}),
	}
}

func parkSealedRuntimeWorkersIntoV1(owner *sealedRuntimeWorkersV1,
	local *sealedInitialization, transport *sealedTransportGateV1,
) (*sealedRuntimeWorkersV1, error) {
	if local == nil || local.raft == nil || local.startup == nil || local.revalidate == nil ||
		owner == nil || transport == nil || owner.transport != transport ||
		!transport.ownsV1(local.raft.trans) || !transport.heldV1() {
		return nil, errSealedRuntimeWorkersV1
	}
	runtime := local.raft
	if runtime.shutdownCh == nil || runtime.applyCh == nil || runtime.fsmMutateCh == nil ||
		runtime.fsmSnapshotCh == nil || runtime.rpcCh == nil || runtime.logs == nil ||
		runtime.stable == nil || runtime.snapshots == nil {
		return nil, errSealedRuntimeWorkersV1
	}
	local.workerMu.Lock()
	if local.workerClaim {
		local.workerMu.Unlock()
		return nil, errSealedRuntimeWorkersV1
	}
	local.workerClaim = true
	local.workerMu.Unlock()
	owner.local, owner.runtime = local, runtime
	owner.startup, owner.revalidate = local.startup, local.revalidate
	if _, bounded := local.startup.Deadline(); !bounded {
		owner.beginCloseV1()
		return owner, errSealedRuntimeWorkersV1
	}
	if err := owner.revalidate(); err != nil {
		owner.beginCloseV1()
		return owner, errors.Join(errSealedRuntimeWorkersV1, err)
	}
	for _, body := range []func(){runtime.run, runtime.runFSM, runtime.runSnapshots} {
		workerBody := body
		runtime.goFunc(func() {
			select {
			case <-runtime.shutdownCh:
				return
			case <-owner.start:
				workerBody()
			}
		})
	}
	// This watcher is registered only after every ordinary runtime worker has
	// joined its wait group. A poisoned local gate owns the same runtime
	// lifetime whether it is observed before or after release.
	// Publish every watcher join channel before either watcher can initiate
	// cleanup. The gate may already have failed during proof revalidation.
	owner.startupStop = make(chan struct{})
	owner.startupDone = make(chan struct{})
	owner.mu.Lock()
	owner.gateWatchDone = make(chan struct{})
	gateWatchDone := owner.gateWatchDone
	owner.mu.Unlock()
	go func() {
		defer close(gateWatchDone)
		<-owner.transport.stop
		if owner.transport.failedV1() {
			// beginClose only launches the joining goroutine, so this watcher
			// returns before that goroutine waits for gateWatchDone.
			owner.beginCloseV1()
		}
	}()
	// Register cancellation only after all wait-group additions. After the
	// successful handoff it must not silently stop the serving lifetime.
	go func() {
		defer close(owner.startupDone)
		select {
		case <-owner.startupStop:
			return
		case <-owner.startup.Done():
			owner.mu.Lock()
			if owner.phase == sealedWorkersReleasedV1 || owner.phase >= sealedWorkersClosingV1 {
				owner.mu.Unlock()
				return
			}
			owner.phase = sealedWorkersClosingV1
			owner.mu.Unlock()
			owner.beginCloseV1()
		}
	}()
	if err := owner.startup.Err(); err != nil {
		owner.beginCloseV1()
		return owner, errors.Join(errSealedRuntimeWorkersV1, err)
	}
	return owner, nil
}

// releaseV1 consumes the original attempt before proof validation. No new
// checkpoint, verifier, store, FSM or deadline can be supplied at release.
func (owner *sealedRuntimeWorkersV1) releaseV1() (*Raft, error) {
	if owner == nil {
		return nil, errSealedRuntimeWorkersV1
	}
	owner.mu.Lock()
	if owner.phase != sealedWorkersHeldV1 {
		owner.mu.Unlock()
		return nil, errSealedRuntimeWorkersV1
	}
	owner.phase = sealedWorkersReleasingV1
	owner.mu.Unlock()
	if err := owner.revalidate(); err != nil {
		owner.beginCloseV1()
		return nil, errors.Join(errSealedRuntimeWorkersV1, err)
	}
	owner.mu.Lock()
	if owner.phase != sealedWorkersReleasingV1 || owner.startup.Err() != nil || owner.local.raft != owner.runtime {
		owner.mu.Unlock()
		owner.beginCloseV1()
		return nil, errSealedRuntimeWorkersV1
	}
	if err := owner.transport.releaseV1(); err != nil {
		owner.mu.Unlock()
		owner.beginCloseV1()
		return nil, errors.Join(errSealedRuntimeWorkersV1, err)
	}
	close(owner.start)
	if err := owner.startup.Err(); err != nil {
		owner.phase = sealedWorkersClosingV1
		owner.mu.Unlock()
		owner.beginCloseV1()
		// A worker may already have run; this is retained cleanup custody,
		// never a claim that the attempt performed no writes.
		return nil, errors.Join(errSealedRuntimeWorkersV1, err)
	}
	owner.phase = sealedWorkersReleasedV1
	owner.stopStartupV1()
	owner.mu.Unlock()
	select {
	case <-owner.startupDone:
		if err := owner.startup.Err(); err != nil {
			owner.beginCloseV1()
			return nil, errors.Join(errSealedRuntimeWorkersV1, err)
		}
	case <-owner.startup.Done():
		owner.beginCloseV1()
		return nil, errors.Join(errSealedRuntimeWorkersV1, owner.startup.Err())
	}
	return owner.runtime, nil
}

func (owner *sealedRuntimeWorkersV1) stopStartupV1() {
	if owner.startupStop != nil {
		owner.stopOnce.Do(func() { close(owner.startupStop) })
	}
}

func (owner *sealedRuntimeWorkersV1) beginCloseV1() {
	owner.closeOnce.Do(func() {
		owner.mu.Lock()
		owner.phase = sealedWorkersClosingV1
		owner.mu.Unlock()
		owner.stopStartupV1()
		var shutdown Future
		if owner.runtime != nil {
			// Signal shutdown synchronously. It is safe at every construction
			// stage and must not wait for a blocking physical close.
			shutdown = owner.runtime.Shutdown()
		}
		// Stop real worker admission immediately, even if physical close blocks.
		// Physical closure still precedes waiting for the resulting join.
		go func() {
			var transportErr error
			if owner.transport != nil {
				transportErr = owner.transport.Close()
			}
			var workerErr error
			if shutdown != nil {
				workerErr = shutdown.Error()
			}
			// A second Shutdown future is already complete and does not join.
			// Always join the originally registered workers themselves too.
			if owner.runtime != nil {
				owner.runtime.waitShutdown()
			}
			if owner.startupDone != nil {
				<-owner.startupDone
			}
			owner.mu.Lock()
			gateWatchDone := owner.gateWatchDone
			owner.mu.Unlock()
			if gateWatchDone != nil {
				<-gateWatchDone
			}
			owner.mu.Lock()
			owner.closeErr = errors.Join(transportErr, workerErr)
			owner.phase = sealedWorkersClosedV1
			owner.mu.Unlock()
			close(owner.closeDone)
		}()
	})
}

// closeV1 joins this same cleanup attempt under a bounded waiting context.
// Timeout or a physical close error retains the owner and forbids reuse.
func (owner *sealedRuntimeWorkersV1) closeV1(ctx context.Context) error {
	if owner == nil || ctx == nil {
		return errSealedRuntimeWorkersV1
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return errSealedRuntimeWorkersV1
	}
	owner.beginCloseV1()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-owner.closeDone:
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return owner.closeErr
	}
}
