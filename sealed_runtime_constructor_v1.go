// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
)

// newSealedRuntimeWorkersV1 prepares one sealed, parked Raft lifetime.
// Its caller retains exclusive custody of supplied inputs.
// It must not reuse a returned owner after any error.
func newSealedRuntimeWorkersV1(
	ctx context.Context,
	conf *Config,
	checkpoint SealedCheckpointV1, logs LogStore, stable StableStore,
	snapshots SnapshotStore, source Transport, app FSM,
	confirmApply func(Log, interface{}) error,
	confirmState func(SealedReadbackV1) error,
) (*sealedRuntimeWorkersV1, error) {
	if ctx == nil || conf == nil || ctx.Err() != nil {
		return nil, errSealedRuntimeWorkersV1
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return nil, errSealedRuntimeWorkersV1
	}
	if _, ok := app.(BatchingFSM); ok {
		return nil, errSealedRuntimeWorkersV1
	}
	if _, ok := app.(ConfigurationStore); ok {
		return nil, errSealedRuntimeWorkersV1
	}
	if logs == nil || stable == nil || snapshots == nil || app == nil ||
		confirmApply == nil || confirmState == nil {
		return nil, errSealedRuntimeWorkersV1
	}

	// Detach all caller-controlled metadata before a source method or
	// callback can observe this attempt.
	copiedConf := *conf
	copiedCheckpoint := checkpoint
	copiedCheckpoint.Snapshot.Configuration =
		checkpoint.Snapshot.Configuration.Clone()
	copiedCheckpoint.Snapshot.Peers = append(
		[]byte(nil), checkpoint.Snapshot.Peers...,
	)
	proof := sealedProof{
		snapshot:   copiedCheckpoint.Snapshot,
		bodyDigest: copiedCheckpoint.BodySHA256,
		tailIndex:  copiedCheckpoint.TailIndex,
		tailTerm:   copiedCheckpoint.TailTerm,
		tailDigest: copiedCheckpoint.TailSHA256,
	}

	transport, gate, err := newSealedTransportGateV1(source)
	if err != nil {
		return nil, errors.Join(errSealedRuntimeWorkersV1, err)
	}
	owner := newSealedRuntimeWorkerOwnerV1(gate)
	owner.logs, owner.stable, owner.snapshots = logs, stable, snapshots
	owner.app, owner.source = app, source
	fail := func(err error) (*sealedRuntimeWorkersV1, error) {
		owner.beginCloseV1()
		return owner, errors.Join(errSealedRuntimeWorkersV1, err)
	}

	runtime, err := newRaftRuntimeV1(
		&copiedConf, app, logs, stable, snapshots, transport,
	)
	if err != nil {
		return fail(err)
	}
	owner.runtime = runtime
	local, err := initializeSealedRaftIntoV1(
		ctx, runtime, proof, logs, stable, snapshots, confirmApply,
		confirmState,
	)
	if err != nil {
		return fail(err)
	}
	owner.local = local
	owner.startup = local.startup
	owner.revalidate = local.revalidate
	if !sealedLocalVoterV1(
		copiedConf.LocalID, gate.LocalAddr(), local.observation.Configuration,
	) {
		return fail(errSealedRuntimeWorkersV1)
	}
	parked, err := parkSealedRuntimeWorkersIntoV1(owner, local, gate)
	if err != nil {
		return fail(err)
	}
	return parked, nil
}

func sealedLocalVoterV1(id ServerID, address ServerAddress,
	configuration Configuration,
) bool {
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter && server.ID == id &&
			server.Address == address {
			return true
		}
	}
	return false
}
