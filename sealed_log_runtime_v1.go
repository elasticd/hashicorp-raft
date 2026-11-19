// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"crypto/sha256"
)

// SealedLogCheckpointV1 identifies the last committed command through which a
// fresh retained FSM must be replayed when no snapshot has been created. It is
// an ephemeral startup proof and is never written to stable storage. A caller
// must derive it from a retained, verified checkpoint; these fields do not
// establish that a command was committed.
type SealedLogCheckpointV1 struct {
	TailIndex  uint64
	TailTerm   uint64
	TailSHA256 [sha256.Size]byte
}

// NewSealedLogRuntimeV1 prepares one sealed, parked Raft lifetime from an
// original log-only startup. The log must begin at index one, the snapshot
// store must be empty, and the checkpoint must identify an exact command in
// the retained log. Entries through the checkpoint are replayed synchronously;
// any later suffix is retained but not applied.
//
// Once transport custody is acquired, a non-nil partial lifetime is returned
// even on error. The caller must close that exact lifetime. This constructor
// does not publish a physical transport, establish a quorum, or authorize a
// production caller.
func NewSealedLogRuntimeV1(
	ctx context.Context,
	conf *Config,
	checkpoint SealedLogCheckpointV1,
	logs LogStore,
	stable StableStore,
	snapshots SnapshotStore,
	source Transport,
	app FSM,
	confirmApply func(Log, interface{}) error,
	confirmState func(SealedReadbackV1) error,
) (*SealedRuntimeV1, error) {
	owner, err := newSealedLogRuntimeWorkersV1(
		ctx, conf, checkpoint, logs, stable, snapshots, source, app,
		confirmApply, confirmState,
	)
	if owner == nil {
		return nil, err
	}
	return &SealedRuntimeV1{owner: owner}, err
}
