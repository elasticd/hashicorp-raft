// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
)

func newSealedLogRuntimeWorkersV1(
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

	copiedConf := *conf
	proof := sealedLogProofV1{
		tailIndex:  checkpoint.TailIndex,
		tailTerm:   checkpoint.TailTerm,
		tailDigest: checkpoint.TailSHA256,
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
	local, err := initializeSealedLogRaftIntoV1(
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
