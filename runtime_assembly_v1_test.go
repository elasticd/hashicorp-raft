// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type runtimeAssemblyFSMV1 struct {
	*MockFSM
	applyCalls, restoreCalls, snapshotCalls atomic.Int64
}

func (fsm *runtimeAssemblyFSMV1) Apply(log *Log) interface{} {
	fsm.applyCalls.Add(1)
	return fsm.MockFSM.Apply(log)
}

func (fsm *runtimeAssemblyFSMV1) Restore(reader io.ReadCloser) error {
	fsm.restoreCalls.Add(1)
	return fsm.MockFSM.Restore(reader)
}

func (fsm *runtimeAssemblyFSMV1) Snapshot() (FSMSnapshot, error) {
	fsm.snapshotCalls.Add(1)
	return fsm.MockFSM.Snapshot()
}

type runtimeAssemblyStoreV1 struct {
	*InmemStore
	storeCalls, stableCalls, deleteCalls atomic.Int64
}

func (store *runtimeAssemblyStoreV1) StoreLog(log *Log) error {
	store.storeCalls.Add(1)
	return store.InmemStore.StoreLog(log)
}

func (store *runtimeAssemblyStoreV1) StoreLogs(logs []*Log) error {
	store.storeCalls.Add(1)
	return store.InmemStore.StoreLogs(logs)
}

func (store *runtimeAssemblyStoreV1) DeleteRange(minimum, maximum uint64) error {
	store.deleteCalls.Add(1)
	return store.InmemStore.DeleteRange(minimum, maximum)
}

func (store *runtimeAssemblyStoreV1) Set(key, value []byte) error {
	store.stableCalls.Add(1)
	return store.InmemStore.Set(key, value)
}

func (store *runtimeAssemblyStoreV1) SetUint64(key []byte, value uint64) error {
	store.stableCalls.Add(1)
	return store.InmemStore.SetUint64(key, value)
}

type runtimeAssemblySnapshotsV1 struct {
	*InmemSnapshotStore
	createCalls, listCalls, openCalls atomic.Int64
}

func (store *runtimeAssemblySnapshotsV1) Create(
	version SnapshotVersion,
	index, term uint64,
	configuration Configuration,
	configurationIndex uint64,
	transport Transport,
) (SnapshotSink, error) {
	store.createCalls.Add(1)
	return store.InmemSnapshotStore.Create(
		version, index, term, configuration, configurationIndex, transport,
	)
}

func (store *runtimeAssemblySnapshotsV1) List() ([]*SnapshotMeta, error) {
	store.listCalls.Add(1)
	return store.InmemSnapshotStore.List()
}

func (store *runtimeAssemblySnapshotsV1) Open(
	identifier string,
) (*SnapshotMeta, io.ReadCloser, error) {
	store.openCalls.Add(1)
	return store.InmemSnapshotStore.Open(identifier)
}

type runtimeAssemblyTransportV1 struct {
	*InmemTransport
	heartbeatCalls, outboundCalls atomic.Int64
}

type runtimeAssemblySealedStoreV1 struct {
	*sealedFixture
	writes atomic.Int64
}

func (store *runtimeAssemblySealedStoreV1) GetUint64(
	key []byte,
) (uint64, error) {
	raw, err := store.Get(key)
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, errSealedInitialization
	}
	return binary.BigEndian.Uint64(raw), nil
}

func (store *runtimeAssemblySealedStoreV1) StoreLog(*Log) error {
	store.writes.Add(1)
	return errSealedInitialization
}

func (store *runtimeAssemblySealedStoreV1) StoreLogs([]*Log) error {
	store.writes.Add(1)
	return errSealedInitialization
}

func (store *runtimeAssemblySealedStoreV1) DeleteRange(uint64, uint64) error {
	store.writes.Add(1)
	return errSealedInitialization
}

func (store *runtimeAssemblySealedStoreV1) Set([]byte, []byte) error {
	store.writes.Add(1)
	return errSealedInitialization
}

func (store *runtimeAssemblySealedStoreV1) SetUint64([]byte, uint64) error {
	store.writes.Add(1)
	return errSealedInitialization
}

type runtimeAssemblySealedSnapshotsV1 struct {
	*sealedFixture
	creates, lists, opens atomic.Int64
}

func (store *runtimeAssemblySealedSnapshotsV1) Create(
	SnapshotVersion,
	uint64,
	uint64,
	Configuration,
	uint64,
	Transport,
) (SnapshotSink, error) {
	store.creates.Add(1)
	return nil, errSealedInitialization
}

func (store *runtimeAssemblySealedSnapshotsV1) List() (
	[]*SnapshotMeta,
	error,
) {
	store.lists.Add(1)
	return store.sealedFixture.List()
}

func (store *runtimeAssemblySealedSnapshotsV1) Open(
	identifier string,
) (*SnapshotMeta, io.ReadCloser, error) {
	store.opens.Add(1)
	return store.sealedFixture.Open(identifier)
}

type runtimeAssemblySealedApplicationV1 struct {
	*sealedTestApplication
	restores, applies atomic.Int64
}

func (app *runtimeAssemblySealedApplicationV1) Restore(
	reader io.ReadCloser,
) error {
	app.restores.Add(1)
	return app.sealedTestApplication.Restore(reader)
}

func (app *runtimeAssemblySealedApplicationV1) Apply(log *Log) interface{} {
	app.applies.Add(1)
	return app.sealedTestApplication.Apply(log)
}

func (transport *runtimeAssemblyTransportV1) SetHeartbeatHandler(func(RPC)) {
	transport.heartbeatCalls.Add(1)
}

func (transport *runtimeAssemblyTransportV1) AppendEntriesPipeline(
	identifier ServerID,
	target ServerAddress,
) (AppendPipeline, error) {
	transport.outboundCalls.Add(1)
	return transport.InmemTransport.AppendEntriesPipeline(identifier, target)
}

func (transport *runtimeAssemblyTransportV1) AppendEntries(
	identifier ServerID,
	target ServerAddress,
	request *AppendEntriesRequest,
	response *AppendEntriesResponse,
) error {
	transport.outboundCalls.Add(1)
	return transport.InmemTransport.AppendEntries(
		identifier, target, request, response,
	)
}

func (transport *runtimeAssemblyTransportV1) RequestVote(
	identifier ServerID,
	target ServerAddress,
	request *RequestVoteRequest,
	response *RequestVoteResponse,
) error {
	transport.outboundCalls.Add(1)
	return transport.InmemTransport.RequestVote(
		identifier, target, request, response,
	)
}

func (transport *runtimeAssemblyTransportV1) RequestPreVote(
	identifier ServerID,
	target ServerAddress,
	request *RequestPreVoteRequest,
	response *RequestPreVoteResponse,
) error {
	transport.outboundCalls.Add(1)
	return transport.InmemTransport.RequestPreVote(
		identifier, target, request, response,
	)
}

func (transport *runtimeAssemblyTransportV1) InstallSnapshot(
	identifier ServerID,
	target ServerAddress,
	request *InstallSnapshotRequest,
	response *InstallSnapshotResponse,
	reader io.Reader,
) error {
	transport.outboundCalls.Add(1)
	return transport.InmemTransport.InstallSnapshot(
		identifier, target, request, response, reader,
	)
}

func (transport *runtimeAssemblyTransportV1) TimeoutNow(
	identifier ServerID,
	target ServerAddress,
	request *TimeoutNowRequest,
	response *TimeoutNowResponse,
) error {
	transport.outboundCalls.Add(1)
	return transport.InmemTransport.TimeoutNow(
		identifier, target, request, response,
	)
}

// TestNewRaftRuntimeV1RetainsInputsWithoutSideEffects proves private runtime
// assembly preserves the exact retained owners without replaying or starting
// any ordinary constructor transition.
func TestNewRaftRuntimeV1RetainsInputsWithoutSideEffects(t *testing.T) {
	t.Parallel()
	address, inmemTransport := NewInmemTransport("node-a")
	transport := &runtimeAssemblyTransportV1{InmemTransport: inmemTransport}
	consumer := inmemTransport.Consumer()
	configuration := Configuration{Servers: []Server{{
		Suffrage: Voter, ID: ServerID(address), Address: address,
	}}}

	inmemStore := NewInmemStore()
	if err := inmemStore.SetUint64(keyCurrentTerm, 6); err != nil {
		t.Fatal(err)
	}
	last := &Log{
		Index: 11, Term: 4, Type: LogCommand,
		Data: []byte("unproved suffix"), Extensions: []byte{42},
	}
	if err := inmemStore.StoreLog(last); err != nil {
		t.Fatal(err)
	}
	store := &runtimeAssemblyStoreV1{InmemStore: inmemStore}

	inmemSnapshots := NewInmemSnapshotStore()
	sink, err := inmemSnapshots.Create(
		1, 8, 3, configuration, 8, inmemTransport,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, writeErr := sink.Write([]byte("retained snapshot")); writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr := sink.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	beforeMeta, beforeBody := runtimeAssemblySnapshotV1(t, inmemSnapshots)
	snapshots := &runtimeAssemblySnapshotsV1{
		InmemSnapshotStore: inmemSnapshots,
	}
	fsm := &runtimeAssemblyFSMV1{MockFSM: &MockFSM{}}
	configurationBefore := configuration.Clone()
	logBefore := *last
	logBefore.Data = bytes.Clone(last.Data)
	logBefore.Extensions = bytes.Clone(last.Extensions)

	conf := DefaultConfig()
	conf.LocalID = ServerID(address)
	conf.HeartbeatTimeout = 5 * time.Millisecond
	conf.ElectionTimeout = 5 * time.Millisecond
	conf.CommitTimeout = time.Millisecond
	conf.LeaderLeaseTimeout = 5 * time.Millisecond
	conf.SnapshotInterval = 5 * time.Millisecond
	runtime, err := newRaftRuntimeV1(
		conf, fsm, store, store, snapshots, transport,
	)
	if err != nil {
		t.Fatal(err)
	}

	if runtime.fsm != fsm || runtime.logs != store ||
		runtime.stable != store || runtime.snapshots != snapshots ||
		runtime.trans != transport || runtime.rpcCh != consumer {
		t.Fatal("runtime assembly replaced a retained owner")
	}
	if runtime.applyCh == nil || runtime.fsmMutateCh == nil ||
		runtime.fsmSnapshotCh == nil || runtime.leaderCh == nil ||
		runtime.configurationChangeCh == nil || runtime.userSnapshotCh == nil ||
		runtime.userRestoreCh == nil || runtime.shutdownCh == nil ||
		runtime.verifyCh == nil || runtime.configurationsCh == nil ||
		runtime.bootstrapCh == nil || runtime.leadershipTransferCh == nil ||
		runtime.leaderNotifyCh == nil || runtime.followerNotifyCh == nil {
		t.Fatal("runtime assembly omitted a worker channel")
	}
	if runtime.State() != Follower || runtime.CurrentTerm() != 6 ||
		runtime.LastIndex() != 11 || runtime.AppliedIndex() != 0 ||
		runtime.CommitIndex() != 0 || runtime.localID != ServerID(address) ||
		runtime.localAddr != address || runtime.configurations.latestIndex != 0 ||
		runtime.configurations.committedIndex != 0 {
		t.Fatal("runtime assembly changed the retained state boundary")
	}

	time.Sleep(20 * time.Millisecond)
	if fsm.applyCalls.Load() != 0 || fsm.restoreCalls.Load() != 0 ||
		fsm.snapshotCalls.Load() != 0 || store.storeCalls.Load() != 0 ||
		store.stableCalls.Load() != 0 || store.deleteCalls.Load() != 0 ||
		snapshots.createCalls.Load() != 0 || snapshots.listCalls.Load() != 0 ||
		snapshots.openCalls.Load() != 0 || transport.heartbeatCalls.Load() != 0 ||
		transport.outboundCalls.Load() != 0 {
		t.Fatal("runtime assembly performed an owned transition")
	}
	if len(consumer) != 0 {
		t.Fatal("runtime assembly admitted peer traffic")
	}

	term, termErr := inmemStore.GetUint64(keyCurrentTerm)
	var logAfter Log
	logErr := inmemStore.GetLog(last.Index, &logAfter)
	afterMeta, afterBody := runtimeAssemblySnapshotV1(t, inmemSnapshots)
	if termErr != nil || term != 6 || logErr != nil ||
		!reflect.DeepEqual(logAfter, logBefore) ||
		!reflect.DeepEqual(afterMeta, beforeMeta) ||
		!bytes.Equal(afterBody, beforeBody) ||
		!reflect.DeepEqual(configuration, configurationBefore) {
		t.Fatal("runtime assembly mutated a retained durable source")
	}
}

func TestInitializeSealedRaftIntoV1KeepsSameRuntime(t *testing.T) {
	t.Parallel()
	source, proof := newSealedFixture()
	expected, _ := newSealedFixture()
	store := &runtimeAssemblySealedStoreV1{sealedFixture: source}
	snapshots := &runtimeAssemblySealedSnapshotsV1{sealedFixture: source}
	_, inmemTransport := NewInmemTransport("old")
	transport := &runtimeAssemblyTransportV1{
		InmemTransport: inmemTransport,
	}
	consumer := inmemTransport.Consumer()
	app := &runtimeAssemblySealedApplicationV1{
		sealedTestApplication: &sealedTestApplication{},
	}
	fsm := appAdapter{app}
	configuration := DecodeConfiguration(source.logs[9].Data)

	config := DefaultConfig()
	config.LocalID = "a"
	config.HeartbeatTimeout = 5 * time.Millisecond
	config.ElectionTimeout = 5 * time.Millisecond
	config.CommitTimeout = time.Millisecond
	config.LeaderLeaseTimeout = 5 * time.Millisecond
	config.SnapshotInterval = 5 * time.Millisecond
	runtime, err := newRaftRuntimeV1(
		config, fsm, store, store, snapshots, transport,
	)
	if err != nil {
		t.Fatal(err)
	}
	applyCh := runtime.applyCh
	fsmMutateCh := runtime.fsmMutateCh
	fsmSnapshotCh := runtime.fsmSnapshotCh
	shutdownCh := runtime.shutdownCh
	rpcCh := runtime.rpcCh
	if snapshots.lists.Load() != 0 || snapshots.opens.Load() != 0 ||
		app.restores.Load() != 0 || app.applies.Load() != 0 {
		t.Fatal("runtime assembly consumed sealed replay inputs")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sealed, err := initializeSealedRaftIntoV1(
		ctx,
		runtime,
		proof,
		store,
		store,
		snapshots,
		confirmSealedFixtureApply,
		func(observation sealedObservation) error {
			return confirmSealedFixtureState(
				app.sealedTestApplication,
				observation,
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if sealed == nil || sealed.raft != runtime || sealed.app != fsm {
		t.Fatal("sealed replay replaced its retained runtime or application")
	}
	if runtime.fsm != fsm || runtime.logs != store ||
		runtime.stable != store || runtime.snapshots != snapshots ||
		runtime.trans != transport || runtime.applyCh != applyCh ||
		runtime.fsmMutateCh != fsmMutateCh ||
		runtime.fsmSnapshotCh != fsmSnapshotCh ||
		runtime.shutdownCh != shutdownCh || runtime.rpcCh != rpcCh ||
		runtime.rpcCh != consumer {
		t.Fatal("sealed replay replaced an assembled runtime owner")
	}
	if runtime.State() != Follower || runtime.CurrentTerm() != 6 ||
		runtime.CommitIndex() != 10 || runtime.AppliedIndex() != 10 ||
		runtime.LastIndex() != 11 || runtime.configurations.committedIndex != 9 ||
		!reflect.DeepEqual(runtime.configurations.committed, configuration) {
		t.Fatal("sealed replay lost the proved runtime boundary")
	}
	if app.restores.Load() != 1 || app.applies.Load() != 1 ||
		!reflect.DeepEqual(app.calls, []uint64{10}) ||
		string(app.data) != "old tail" {
		t.Fatal("sealed replay restored twice or applied outside the proved tail")
	}
	if snapshots.lists.Load() != 2 || snapshots.opens.Load() != 2 ||
		snapshots.creates.Load() != 0 || store.writes.Load() != 0 {
		t.Fatal("sealed replay mutated or unexpectedly reopened a retained source")
	}

	time.Sleep(20 * time.Millisecond)
	if transport.heartbeatCalls.Load() != 0 ||
		transport.outboundCalls.Load() != 0 || len(consumer) != 0 ||
		app.restores.Load() != 1 || app.applies.Load() != 1 ||
		store.writes.Load() != 0 || snapshots.creates.Load() != 0 ||
		!reflect.DeepEqual(source, expected) {
		t.Fatal("sealed replay started workers, peer access, or durable mutation")
	}
}

func runtimeAssemblySnapshotV1(
	t *testing.T,
	store *InmemSnapshotStore,
) (SnapshotMeta, []byte) {
	t.Helper()
	list, err := store.List()
	if err != nil || len(list) != 1 {
		t.Fatalf("snapshot inventory: %v", err)
	}
	meta, reader, err := store.Open(list[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("snapshot body: %v %v", readErr, closeErr)
	}
	copy := *meta
	copy.Peers = bytes.Clone(meta.Peers)
	copy.Configuration = meta.Configuration.Clone()
	return copy, body
}
