// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import "context"

// SealedCheckpointV1 identifies an already-proved local snapshot and
// committed command. A caller must derive it from a retained, verified
// checkpoint; these fields do not establish that a command was committed.
type SealedCheckpointV1 struct {
	Snapshot   SnapshotMeta
	BodySHA256 [32]byte
	TailIndex  uint64
	TailTerm   uint64
	TailSHA256 [32]byte
}

// SealedReadbackV1 reports the local library positions and configuration
// for comparison with the independently replayed application state.
// It does not establish membership authority or prove a quorum.
type SealedReadbackV1 = sealedObservation

// SealedLogReaderV1 reads retained Raft log entries without mutating them.
type SealedLogReaderV1 = sealedLogReader

// SealedStableReaderV1 reads the raw persisted election records. The term
// records must be eight-byte big-endian values. A missing record must return
// the store's "not found" error; an empty record is malformed.
type SealedStableReaderV1 = sealedStableReader

// SealedSnapshotReaderV1 opens the already-selected snapshot and metadata.
// List must expose exactly that snapshot, not the entire store inventory.
type SealedSnapshotReaderV1 = sealedSnapshotReader

// SealedLocalRaftV1 retains the locally initialized node and application.
// It exposes no start operation, transport, or writable storage.
type SealedLocalRaftV1 struct {
	sealed *sealedInitialization
}

// NewSealedLocalRaftV1 initializes Raft through the exact proved tail under
// a bounded context without starting it. It synchronously restores the
// snapshot and waits for per-command and final application confirmation.
//
// The caller must retain and serialize access to the original storage
// and own a fresh, private FSM. The FSM may be partially initialized
// on error; it must not be reused or published. The function receives no
// transport, mutates no durable record and does not truncate a suffix.
// Only ordinary single-entry FSMs are supported: batching and configuration
// store FSMs are refused before restore. The FSM and callbacks must not
// mutate the retained sources or publish authority during initialization.
// The context is checked between synchronous calls; it cannot interrupt a
// blocked store, FSM, or confirmation callback.
func NewSealedLocalRaftV1(ctx context.Context, checkpoint SealedCheckpointV1,
	logs SealedLogReaderV1, stable SealedStableReaderV1, snapshots SealedSnapshotReaderV1, app FSM,
	confirmApply func(Log, interface{}) error, confirmState func(SealedReadbackV1) error,
) (*SealedLocalRaftV1, error) {
	if _, ok := app.(BatchingFSM); ok {
		return nil, errSealedInitialization
	}
	if _, ok := app.(ConfigurationStore); ok {
		return nil, errSealedInitialization
	}
	proof := sealedProof{
		snapshot: checkpoint.Snapshot, bodyDigest: checkpoint.BodySHA256,
		tailIndex: checkpoint.TailIndex, tailTerm: checkpoint.TailTerm, tailDigest: checkpoint.TailSHA256,
	}
	sealed, err := initializeSealedRaft(ctx, proof, logs, stable, snapshots, app, confirmApply, confirmState)
	if err != nil {
		return nil, err
	}
	return &SealedLocalRaftV1{sealed: sealed}, nil
}

// Readback returns the sealed library facts without exposing the node.
// The configuration is detached from the retained state.
func (local *SealedLocalRaftV1) Readback() (SealedReadbackV1, bool) {
	if local == nil || local.sealed == nil {
		return SealedReadbackV1{}, false
	}
	observation := local.sealed.observation
	observation.Configuration = observation.Configuration.Clone()
	return observation, true
}
