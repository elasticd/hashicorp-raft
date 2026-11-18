// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import "context"

// SealedRuntimeV1 is an opaque, single-use sealed Raft lifetime.
//
// Value copies alias the same retained worker owner. The original deferred
// authenticated transport and guarded FSM/snapshot owner remain outer
// responsibilities.
type SealedRuntimeV1 struct {
	owner *sealedRuntimeWorkersV1
}

// NewSealedRuntimeV1 prepares one sealed, parked Raft lifetime from the
// original constructor inputs. A non-nil partial lifetime is always returned,
// including when construction returns an error, so its caller retains cleanup
// custody. It does not authenticate physical publication, establish a quorum,
// establish health or resource readiness, or authorize an elasticd production
// caller.
func NewSealedRuntimeV1(
	ctx context.Context,
	conf *Config,
	checkpoint SealedCheckpointV1,
	logs LogStore,
	stable StableStore,
	snapshots SnapshotStore,
	source Transport,
	app FSM,
	confirmApply func(Log, interface{}) error,
	confirmState func(SealedReadbackV1) error,
) (*SealedRuntimeV1, error) {
	owner, err := newSealedRuntimeWorkersV1(
		ctx, conf, checkpoint, logs, stable, snapshots, source, app,
		confirmApply, confirmState,
	)
	if owner == nil {
		return nil, err
	}
	return &SealedRuntimeV1{owner: owner}, err
}

// ReleaseV1 starts consensus for this sealed lifetime and may write state.
// It is not authenticated physical publication, a quorum, health, resource
// readiness, or permission for an elasticd production caller. The original
// deferred authenticated transport and guarded FSM/snapshot owner remain outer
// responsibilities. A returned Raft and its Shutdown future never supersede
// this wrapper's physical transport cleanup and actual worker join.
func (runtime *SealedRuntimeV1) ReleaseV1() (*Raft, error) {
	if runtime == nil || runtime.owner == nil {
		return nil, errSealedRuntimeWorkersV1
	}
	return runtime.owner.releaseV1()
}

// CloseV1 retains and joins cleanup for this sealed lifetime. Its bounded
// context limits waiting only; it cannot renew startup, abandon cleanup, erase
// errors, replace a source, or permit reuse. The original deferred
// authenticated transport and guarded FSM/snapshot owner remain outer
// responsibilities. A returned Raft and its Shutdown future never supersede
// this wrapper's physical transport cleanup and actual worker join.
func (runtime *SealedRuntimeV1) CloseV1(ctx context.Context) error {
	if runtime == nil || runtime.owner == nil {
		return errSealedRuntimeWorkersV1
	}
	return runtime.owner.closeV1(ctx)
}
