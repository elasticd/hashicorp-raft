// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
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
