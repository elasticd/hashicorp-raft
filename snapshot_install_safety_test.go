// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/stretchr/testify/require"
)

func TestRaftInstallSnapshotDiscardsDivergentLogAndCachedTail(t *testing.T) {
	logs := NewInmemStore()
	for index := uint64(1); index <= 8; index++ {
		term := uint64(2)
		if index >= 3 {
			term = 99
		}
		require.NoError(t, logs.StoreLog(&Log{Index: index, Term: term}))
	}

	raft, fsm, snapshots, transport := newSnapshotSafetyRaft(t, logs)
	response := installSnapshotForSafetyTest(t, raft, transport.LocalAddr(), 3, 2)
	require.NoError(t, response.Error)
	require.True(t, response.Response.(*InstallSnapshotResponse).Success)
	require.Len(t, fsm.Logs(), 2)

	first, err := logs.FirstIndex()
	require.NoError(t, err)
	require.Zero(t, first)
	last, err := logs.LastIndex()
	require.NoError(t, err)
	require.Zero(t, last)

	lastLogIndex, lastLogTerm := raft.getLastLog()
	require.Zero(t, lastLogIndex)
	require.Zero(t, lastLogTerm)
	lastIndex, lastTerm := raft.getLastEntry()
	require.Equal(t, uint64(3), lastIndex)
	require.Equal(t, uint64(2), lastTerm)

	metadata, err := snapshots.List()
	require.NoError(t, err)
	require.Len(t, metadata, 1)
}

func TestRaftInstallSnapshotDiscardsLogWithoutBoundaryEntry(t *testing.T) {
	logs := NewInmemStore()
	for index := uint64(1); index <= 8; index++ {
		require.NoError(t, logs.StoreLog(&Log{Index: index, Term: 99}))
	}

	raft, _, _, transport := newSnapshotSafetyRaft(t, logs)
	require.NoError(t, logs.DeleteRange(1, 3))
	response := installSnapshotForSafetyTest(t, raft, transport.LocalAddr(), 3, 2)
	require.NoError(t, response.Error)
	require.True(t, response.Response.(*InstallSnapshotResponse).Success)

	last, err := logs.LastIndex()
	require.NoError(t, err)
	require.Zero(t, last)
	lastLogIndex, lastLogTerm := raft.getLastLog()
	require.Zero(t, lastLogIndex)
	require.Zero(t, lastLogTerm)
}

func TestRaftInstallSnapshotKeepsMatchingLogAndSuffix(t *testing.T) {
	logs := NewInmemStore()
	for index := uint64(1); index <= 5; index++ {
		term := uint64(2)
		if index > 3 {
			term = 99
		}
		require.NoError(t, logs.StoreLog(&Log{Index: index, Term: term}))
	}

	raft, _, _, transport := newSnapshotSafetyRaft(t, logs)
	response := installSnapshotForSafetyTest(t, raft, transport.LocalAddr(), 3, 2)
	require.NoError(t, response.Error)
	require.True(t, response.Response.(*InstallSnapshotResponse).Success)

	for index := uint64(1); index <= 5; index++ {
		var entry Log
		require.NoError(t, logs.GetLog(index, &entry))
		if index > 3 {
			require.Equal(t, uint64(99), entry.Term)
		}
	}
	lastLogIndex, lastLogTerm := raft.getLastLog()
	require.Equal(t, uint64(5), lastLogIndex)
	require.Equal(t, uint64(99), lastLogTerm)
}

func TestRaftInstallSnapshotResetsCachedTailAfterMonotonicCleanup(t *testing.T) {
	base := NewInmemStore()
	for index := uint64(1); index <= 5; index++ {
		require.NoError(t, base.StoreLog(&Log{Index: index, Term: 2}))
	}
	logs := &MockMonotonicLogStore{s: base}

	raft, _, _, transport := newSnapshotSafetyRaft(t, logs)
	response := installSnapshotForSafetyTest(t, raft, transport.LocalAddr(), 3, 2)
	require.NoError(t, response.Error)
	require.True(t, response.Response.(*InstallSnapshotResponse).Success)

	last, err := base.LastIndex()
	require.NoError(t, err)
	require.Zero(t, last)
	lastLogIndex, lastLogTerm := raft.getLastLog()
	require.Zero(t, lastLogIndex)
	require.Zero(t, lastLogTerm)
	lastIndex, lastTerm := raft.getLastEntry()
	require.Equal(t, uint64(3), lastIndex)
	require.Equal(t, uint64(2), lastTerm)
}

func TestRaftInstallSnapshotPropagatesLogDeletionFailure(t *testing.T) {
	deleteErr := errors.New("injected delete failure")
	base := NewInmemStore()
	for index := uint64(1); index <= 8; index++ {
		term := uint64(2)
		if index >= 3 {
			term = 99
		}
		require.NoError(t, base.StoreLog(&Log{Index: index, Term: term}))
	}
	logs := &failingDeleteLogStore{InmemStore: base, err: deleteErr}

	raft, fsm, _, transport := newSnapshotSafetyRaft(t, logs)
	response := installSnapshotForSafetyTest(t, raft, transport.LocalAddr(), 3, 2)
	require.ErrorIs(t, response.Error, deleteErr)
	require.False(t, response.Response.(*InstallSnapshotResponse).Success)
	require.Empty(t, fsm.Logs())
	require.Equal(t, 1, logs.calls)

	lastLogIndex, lastLogTerm := raft.getLastLog()
	require.Equal(t, uint64(8), lastLogIndex)
	require.Equal(t, uint64(99), lastLogTerm)
}

func TestRaftStartupCompletesFinalizedSnapshotReconciliation(t *testing.T) {
	deleteErr := errors.New("injected delete failure")
	base := NewInmemStore()
	for index := uint64(1); index <= 8; index++ {
		term := uint64(2)
		if index >= 3 {
			term = 99
		}
		require.NoError(t, base.StoreLog(&Log{Index: index, Term: term}))
	}
	logs := &failingDeleteLogStore{InmemStore: base, err: deleteErr}
	stable := NewInmemStore()
	snapshots := NewInmemSnapshotStore()
	config := inmemConfig(t)
	config.LocalID = "follower"
	config.HeartbeatTimeout = time.Hour
	config.ElectionTimeout = time.Hour
	config.LeaderLeaseTimeout = time.Hour

	_, firstTransport := NewInmemTransport("")
	firstFSM := &MockFSM{}
	firstRaft, err := NewRaft(
		config,
		firstFSM,
		logs,
		stable,
		snapshots,
		firstTransport,
	)
	require.NoError(t, err)
	response := installSnapshotForSafetyTest(
		t,
		firstRaft,
		firstTransport.LocalAddr(),
		3,
		2,
	)
	require.ErrorIs(t, response.Error, deleteErr)
	require.False(t, response.Response.(*InstallSnapshotResponse).Success)
	require.NoError(t, firstRaft.Shutdown().Error())

	metadata, err := snapshots.List()
	require.NoError(t, err)
	require.Len(t, metadata, 1)

	_, failingRestartTransport := NewInmemTransport("")
	_, err = NewRaft(
		config,
		&MockFSM{},
		logs,
		stable,
		snapshots,
		failingRestartTransport,
	)
	require.ErrorIs(t, err, deleteErr)

	logs.err = nil
	_, recoveredTransport := NewInmemTransport("")
	recoveredFSM := &MockFSM{}
	recoveredRaft, err := NewRaft(
		config,
		recoveredFSM,
		logs,
		stable,
		snapshots,
		recoveredTransport,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, recoveredRaft.Shutdown().Error())
	})
	require.Len(t, recoveredFSM.Logs(), 2)

	first, err := logs.FirstIndex()
	require.NoError(t, err)
	require.Zero(t, first)
	last, err := logs.LastIndex()
	require.NoError(t, err)
	require.Zero(t, last)
	lastIndex, lastTerm := recoveredRaft.getLastEntry()
	require.Equal(t, uint64(3), lastIndex)
	require.Equal(t, uint64(2), lastTerm)
}

type failingDeleteLogStore struct {
	*InmemStore
	err   error
	calls int
}

func (store *failingDeleteLogStore) DeleteRange(first, last uint64) error {
	store.calls++
	if store.err != nil {
		return store.err
	}
	return store.InmemStore.DeleteRange(first, last)
}

func newSnapshotSafetyRaft(
	t *testing.T,
	logs LogStore,
) (*Raft, *MockFSM, *InmemSnapshotStore, *InmemTransport) {
	t.Helper()
	config := inmemConfig(t)
	config.LocalID = "follower"
	config.TrailingLogs = 20
	config.HeartbeatTimeout = time.Hour
	config.ElectionTimeout = time.Hour
	config.LeaderLeaseTimeout = time.Hour
	_, transport := NewInmemTransport("")
	fsm := &MockFSM{}
	snapshots := NewInmemSnapshotStore()
	raft, err := NewRaft(
		config,
		fsm,
		logs,
		NewInmemStore(),
		snapshots,
		transport,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, raft.Shutdown().Error())
	})
	return raft, fsm, snapshots, transport
}

func installSnapshotForSafetyTest(
	t *testing.T,
	raft *Raft,
	leader ServerAddress,
	lastLogIndex uint64,
	lastLogTerm uint64,
) RPCResponse {
	t.Helper()

	var data bytes.Buffer
	encoder := codec.NewEncoder(&data, &codec.MsgpackHandle{})
	require.NoError(t, encoder.Encode([][]byte{[]byte("a"), []byte("b")}))

	request := &InstallSnapshotRequest{
		RPCHeader:          RPCHeader{ProtocolVersion: ProtocolVersionMax},
		SnapshotVersion:    SnapshotVersionMax,
		Term:               3,
		Leader:             []byte(leader),
		LastLogIndex:       lastLogIndex,
		LastLogTerm:        lastLogTerm,
		Configuration:      EncodeConfiguration(Configuration{Servers: []Server{{Suffrage: Voter, ID: "follower", Address: leader}}}),
		ConfigurationIndex: lastLogIndex,
		Size:               int64(data.Len()),
	}
	responses := make(chan RPCResponse, 1)
	raft.processRPC(RPC{
		Command:  request,
		Reader:   io.NopCloser(bytes.NewReader(data.Bytes())),
		RespChan: responses,
	})
	return <-responses
}
