// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"
)

type sealedLogRuntimeFixtureV1 struct {
	checkpoint SealedLogCheckpointV1
	store      *runtimeAssemblyStoreV1
	snapshots  *runtimeAssemblySnapshotsV1
	source     *sealedWorkersTransportV1
	app        *runtimeAssemblyFSMV1
	conf       *Config
	config     Configuration
}

func newSealedLogRuntimeFixtureV1(t *testing.T) *sealedLogRuntimeFixtureV1 {
	t.Helper()
	address, transport := NewInmemTransport("node-a")
	configuration := Configuration{Servers: []Server{{
		Suffrage: Voter, ID: ServerID(address), Address: address,
	}}}
	rawStore := NewInmemStore()
	term := make([]byte, 8)
	binary.BigEndian.PutUint64(term, 2)
	if err := rawStore.Set(keyCurrentTerm, term); err != nil {
		t.Fatal(err)
	}
	if err := rawStore.SetUint64(keyCurrentTerm, 2); err != nil {
		t.Fatal(err)
	}
	entries := []*Log{
		{Index: 1, Term: 1, Type: LogConfiguration,
			Data: EncodeConfiguration(configuration)},
		{Index: 2, Term: 1, Type: LogCommand, Data: []byte("one")},
		{Index: 3, Term: 2, Type: LogNoop},
		{Index: 4, Term: 2, Type: LogCommand, Data: []byte("tail")},
		{Index: 5, Term: 2, Type: LogCommand,
			Data: []byte("unproved"), Extensions: []byte{42}},
	}
	if err := rawStore.StoreLogs(entries); err != nil {
		t.Fatal(err)
	}
	rawSnapshots := NewInmemSnapshotStore()
	conf := DefaultConfig()
	conf.LocalID = ServerID(address)
	conf.HeartbeatTimeout = time.Hour
	conf.ElectionTimeout = time.Hour
	conf.LeaderLeaseTimeout = time.Hour
	conf.SnapshotInterval = time.Hour
	return &sealedLogRuntimeFixtureV1{
		checkpoint: SealedLogCheckpointV1{
			TailIndex: 4, TailTerm: 2,
			TailSHA256: sha256.Sum256([]byte("tail")),
		},
		store: &runtimeAssemblyStoreV1{InmemStore: rawStore},
		snapshots: &runtimeAssemblySnapshotsV1{
			InmemSnapshotStore: rawSnapshots,
		},
		source: &sealedWorkersTransportV1{
			runtimeAssemblyTransportV1: &runtimeAssemblyTransportV1{
				InmemTransport: transport,
			},
		},
		app:    &runtimeAssemblyFSMV1{MockFSM: &MockFSM{}},
		conf:   conf,
		config: configuration,
	}
}

func (fixture *sealedLogRuntimeFixtureV1) prepare(
	t *testing.T,
) (*SealedRuntimeV1, context.CancelFunc, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	runtime, err := NewSealedLogRuntimeV1(
		ctx, fixture.conf, fixture.checkpoint, fixture.store,
		fixture.store, fixture.snapshots, fixture.source, fixture.app,
		func(log Log, result interface{}) error {
			position, ok := result.(int)
			if !ok || (log.Index == 2 && position != 1) ||
				(log.Index == 4 && position != 2) {
				return errors.New("wrong applied command result")
			}
			return nil
		},
		func(got SealedReadbackV1) error {
			if got.CommitIndex != 4 || got.AppliedIndex != 4 ||
				got.SnapshotIndex != 0 || got.LastLogIndex != 5 ||
				got.TailTerm != 2 || got.ElectionTerm != 2 ||
				got.ConfigurationIndex != 1 ||
				!reflect.DeepEqual(got.Configuration, fixture.config) {
				return errors.New("wrong log-only readback")
			}
			return nil
		},
	)
	return runtime, cancel, err
}

func closeSealedLogRuntimeV1(
	t *testing.T, runtime *SealedRuntimeV1,
) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return runtime.CloseV1(ctx)
}

func TestNewSealedLogRuntimeV1ReplaysAndParksSameRuntime(t *testing.T) {
	fixture := newSealedLogRuntimeFixtureV1(t)
	before := make(map[uint64]Log)
	for index := uint64(1); index <= 5; index++ {
		var entry Log
		if err := fixture.store.GetLog(index, &entry); err != nil {
			t.Fatal(err)
		}
		before[index] = cloneSealedLog(entry)
	}
	runtime, cancel, err := fixture.prepare(t)
	defer cancel()
	if err != nil || runtime == nil || runtime.owner == nil {
		t.Fatalf("prepare: runtime=%p err=%v", runtime, err)
	}
	owner := runtime.owner
	if owner.phase != sealedWorkersHeldV1 || owner.runtime == nil ||
		owner.runtime.fsm != fixture.app ||
		owner.runtime.logs != fixture.store ||
		owner.runtime.stable != fixture.store ||
		owner.runtime.snapshots != fixture.snapshots ||
		owner.source != fixture.source {
		t.Fatal("log-only constructor replaced a retained owner")
	}
	if fixture.app.restoreCalls.Load() != 0 ||
		fixture.app.applyCalls.Load() != 2 ||
		fixture.app.snapshotCalls.Load() != 0 ||
		fixture.store.storeCalls.Load() != 0 ||
		fixture.store.stableCalls.Load() != 0 ||
		fixture.store.deleteCalls.Load() != 0 ||
		fixture.snapshots.createCalls.Load() != 0 ||
		fixture.snapshots.openCalls.Load() != 0 ||
		fixture.source.outboundCalls.Load() != 0 ||
		fixture.source.heartbeatNonNil.Load() != 0 {
		t.Fatal("log-only constructor performed serving or durable work")
	}
	if got := fixture.app.Logs(); !reflect.DeepEqual(
		got, [][]byte{[]byte("one"), []byte("tail")},
	) {
		t.Fatalf("wrong replay: %q", got)
	}
	for index := uint64(1); index <= 5; index++ {
		var got Log
		if err := fixture.store.GetLog(index, &got); err != nil ||
			!reflect.DeepEqual(cloneSealedLog(got), before[index]) {
			t.Fatalf("log %d changed: %#v %v", index, got, err)
		}
	}
	original := owner.runtime
	released, err := runtime.ReleaseV1()
	if err != nil || released != original {
		t.Fatalf("release replaced runtime: got=%p want=%p err=%v",
			released, original, err)
	}
	if err := closeSealedLogRuntimeV1(t, runtime); err != nil {
		t.Fatal(err)
	}
	if fixture.source.closes.Load() != 1 {
		t.Fatalf("physical close count: %d", fixture.source.closes.Load())
	}
}

func TestNewSealedLogRuntimeV1RefencesSourcesBeforeRelease(t *testing.T) {
	for _, mutation := range []string{"log", "stable", "snapshot"} {
		t.Run(mutation, func(t *testing.T) {
			fixture := newSealedLogRuntimeFixtureV1(t)
			runtime, cancel, err := fixture.prepare(t)
			defer cancel()
			if err != nil || runtime == nil {
				t.Fatalf("prepare: runtime=%p err=%v", runtime, err)
			}
			switch mutation {
			case "log":
				if err := fixture.store.InmemStore.StoreLog(&Log{
					Index: 5, Term: 2, Type: LogCommand,
					Data: []byte("changed"),
				}); err != nil {
					t.Fatal(err)
				}
			case "stable":
				term := make([]byte, 8)
				binary.BigEndian.PutUint64(term, 3)
				if err := fixture.store.InmemStore.Set(
					keyCurrentTerm, term,
				); err != nil {
					t.Fatal(err)
				}
			case "snapshot":
				sink, err := fixture.snapshots.InmemSnapshotStore.Create(
					1, 4, 2, fixture.config, 1,
					fixture.source.InmemTransport,
				)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := sink.Write([]byte("unexpected")); err != nil {
					t.Fatal(err)
				}
				if err := sink.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if node, err := runtime.ReleaseV1(); node != nil ||
				!errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatalf("drift released: node=%p err=%v", node, err)
			}
			if err := closeSealedLogRuntimeV1(t, runtime); err != nil {
				t.Fatal(err)
			}
			if fixture.source.closes.Load() != 1 ||
				fixture.source.outboundCalls.Load() != 0 {
				t.Fatal("drift refusal lost cleanup or emitted RPC")
			}
		})
	}
}

func TestNewSealedLogRuntimeV1RefusesMalformedProofWithCustody(t *testing.T) {
	for _, malformed := range []string{
		"digest", "snapshot", "first", "command-before-configuration",
	} {
		t.Run(malformed, func(t *testing.T) {
			fixture := newSealedLogRuntimeFixtureV1(t)
			switch malformed {
			case "digest":
				fixture.checkpoint.TailSHA256 =
					sha256.Sum256([]byte("wrong"))
			case "snapshot":
				sink, err := fixture.snapshots.InmemSnapshotStore.Create(
					1, 4, 2, fixture.config, 1,
					fixture.source.InmemTransport,
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := sink.Close(); err != nil {
					t.Fatal(err)
				}
			case "first":
				if err := fixture.store.InmemStore.DeleteRange(1, 1); err != nil {
					t.Fatal(err)
				}
			case "command-before-configuration":
				if err := fixture.store.InmemStore.StoreLog(&Log{
					Index: 1, Term: 1, Type: LogCommand,
					Data: []byte("before configuration"),
				}); err != nil {
					t.Fatal(err)
				}
				if err := fixture.store.InmemStore.StoreLog(&Log{
					Index: 3, Term: 2, Type: LogConfiguration,
					Data: EncodeConfiguration(fixture.config),
				}); err != nil {
					t.Fatal(err)
				}
			}
			runtime, cancel, err := fixture.prepare(t)
			defer cancel()
			if runtime == nil || runtime.owner == nil ||
				!errors.Is(err, errSealedRuntimeWorkersV1) {
				t.Fatalf("malformed proof lost custody: %p %v", runtime, err)
			}
			if err := closeSealedLogRuntimeV1(t, runtime); err != nil {
				t.Fatal(err)
			}
			if fixture.source.closes.Load() != 1 ||
				fixture.app.applyCalls.Load() != 0 ||
				fixture.app.restoreCalls.Load() != 0 ||
				fixture.store.storeCalls.Load() != 0 ||
				fixture.store.stableCalls.Load() != 0 ||
				fixture.source.outboundCalls.Load() != 0 {
				t.Fatal("malformed proof escaped cleanup or restored")
			}
		})
	}
}

func TestNewSealedLogRuntimeV1RefusesUnboundedContextBeforeCustody(
	t *testing.T,
) {
	fixture := newSealedLogRuntimeFixtureV1(t)
	runtime, err := NewSealedLogRuntimeV1(
		context.Background(), fixture.conf, fixture.checkpoint,
		fixture.store, fixture.store, fixture.snapshots, fixture.source,
		fixture.app, func(Log, interface{}) error { return nil },
		func(SealedReadbackV1) error { return nil },
	)
	if runtime != nil || !errors.Is(err, errSealedRuntimeWorkersV1) ||
		fixture.source.closes.Load() != 0 ||
		fixture.checkpoint.TailSHA256 != sha256.Sum256([]byte("tail")) {
		t.Fatalf("unbounded attempt acquired custody: %p %v", runtime, err)
	}
}
