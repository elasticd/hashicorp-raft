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

// SealedStableReaderV1 reads retained election records without writing them.
type SealedStableReaderV1 = sealedStableReader

// SealedSnapshotReaderV1 opens the already-selected snapshot and metadata.
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
func NewSealedLocalRaftV1(ctx context.Context, checkpoint SealedCheckpointV1,
	logs SealedLogReaderV1, stable SealedStableReaderV1, snapshots SealedSnapshotReaderV1, app FSM,
	confirmApply func(Log, interface{}) error, confirmState func(SealedReadbackV1) error,
) (*SealedLocalRaftV1, error) {
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
