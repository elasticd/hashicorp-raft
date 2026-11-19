// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
)

type sealedLogProofV1 struct {
	tailIndex  uint64
	tailTerm   uint64
	tailDigest [sha256.Size]byte
}

type sealedLogHistoryV1 struct {
	first, last uint64
	entries     []Log
}

func initializeSealedLogRaftIntoV1(
	ctx context.Context,
	runtime *Raft,
	proof sealedLogProofV1,
	logs sealedLogReader,
	stable sealedStableReader,
	snapshots sealedSnapshotReader,
	confirmApply func(Log, interface{}) error,
	confirmState func(sealedObservation) error,
) (*sealedInitialization, error) {
	if ctx == nil || ctx.Err() != nil || runtime == nil ||
		runtime.fsm == nil || logs == nil || stable == nil ||
		snapshots == nil || confirmApply == nil || confirmState == nil {
		return nil, errSealedInitialization
	}
	if _, bounded := ctx.Deadline(); !bounded || proof.tailIndex == 0 ||
		proof.tailTerm == 0 {
		return nil, errSealedInitialization
	}
	election, err := readSealedElection(stable)
	if err != nil {
		return nil, err
	}
	history, err := captureSealedLogHistoryV1(
		ctx, logs, proof, election.term,
	)
	if err != nil {
		return nil, err
	}
	if err := checkEmptySealedSnapshotsV1(snapshots); err != nil {
		return nil, err
	}
	observation, err := replaySealedLogHistoryV1(
		ctx, runtime, proof, history, election.term, confirmApply,
	)
	if err != nil {
		return nil, err
	}
	confirm := observation
	confirm.Configuration = observation.Configuration.Clone()
	if err := confirmState(confirm); err != nil {
		return nil, errors.Join(errSealedInitialization, err)
	}
	if err := refenceSealedLogSourcesV1(
		ctx, proof, history, election, logs, stable, snapshots,
	); err != nil {
		return nil, err
	}

	originalFSM, originalLogs, originalStable :=
		runtime.fsm, runtime.logs, runtime.stable
	originalSnapshots, originalTransport :=
		runtime.snapshots, runtime.trans
	originalSnapshotIndex, originalSnapshotTerm :=
		runtime.getLastSnapshot()
	originalLogIndex, originalLogTerm := runtime.getLastLog()
	originalConfig := runtime.conf.Load()
	originalLocalID, originalLocalAddr := runtime.localID, runtime.localAddr
	originalProtocol := runtime.protocolVersion
	originalLatest := runtime.configurations.latest.Clone()
	originalLatestIndex := runtime.configurations.latestIndex
	verifyRuntime := func() bool {
		snapshotIndex, snapshotTerm := runtime.getLastSnapshot()
		logIndex, logTerm := runtime.getLastLog()
		return runtime.State() == Follower &&
			runtime.localID == originalLocalID &&
			runtime.localAddr == originalLocalAddr &&
			runtime.protocolVersion == originalProtocol &&
			snapshotIndex == originalSnapshotIndex &&
			snapshotTerm == originalSnapshotTerm &&
			logIndex == originalLogIndex && logTerm == originalLogTerm &&
			reflect.DeepEqual(runtime.conf.Load(), originalConfig) &&
			reflect.DeepEqual(
				observeSealedRuntimeV1(runtime, proof.tailTerm),
				observation,
			) &&
			runtime.configurations.latestIndex == originalLatestIndex &&
			reflect.DeepEqual(
				runtime.configurations.latest, originalLatest,
			) &&
			sameSealedInputV1(runtime.fsm, originalFSM) &&
			sameSealedInputV1(runtime.logs, originalLogs) &&
			sameSealedInputV1(runtime.stable, originalStable) &&
			sameSealedInputV1(runtime.snapshots, originalSnapshots) &&
			sameSealedInputV1(runtime.trans, originalTransport)
	}
	retained := &sealedInitialization{
		raft: runtime, app: runtime.fsm,
		observation: observation, startup: ctx,
	}
	retained.revalidate = func() error {
		if retained.raft != runtime ||
			!sameSealedInputV1(retained.startup, ctx) || !verifyRuntime() {
			return errSealedInitialization
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(errSealedInitialization, err)
		}
		if err := refenceSealedLogSourcesV1(
			ctx, proof, history, election, logs, stable, snapshots,
		); err != nil {
			return err
		}
		confirmed := observation
		confirmed.Configuration = observation.Configuration.Clone()
		if err := confirmState(confirmed); err != nil {
			return errors.Join(errSealedInitialization, err)
		}
		if !verifyRuntime() {
			return errSealedInitialization
		}
		return refenceSealedLogSourcesV1(
			ctx, proof, history, election, logs, stable, snapshots,
		)
	}
	return retained, nil
}

func captureSealedLogHistoryV1(
	ctx context.Context,
	logs sealedLogReader,
	proof sealedLogProofV1,
	electionTerm uint64,
) (sealedLogHistoryV1, error) {
	first, firstErr := logs.FirstIndex()
	last, lastErr := logs.LastIndex()
	if firstErr != nil || lastErr != nil || first != 1 ||
		last < proof.tailIndex || last > sealedEntryLimit {
		return sealedLogHistoryV1{}, errors.Join(
			errSealedInitialization, firstErr, lastErr,
		)
	}
	history := sealedLogHistoryV1{first: first, last: last}
	var previousTerm uint64
	remaining := sealedLogBytes
	for index := first; ; index++ {
		if err := ctx.Err(); err != nil {
			return sealedLogHistoryV1{}, errors.Join(
				errSealedInitialization, err,
			)
		}
		var entry Log
		if err := logs.GetLog(index, &entry); err != nil {
			return sealedLogHistoryV1{}, errors.Join(
				errSealedInitialization, err,
			)
		}
		if entry.Index != index || entry.Term == 0 ||
			entry.Term < previousTerm || entry.Term > electionTerm ||
			(index == 1 && entry.Type != LogConfiguration) ||
			len(entry.Data) > remaining ||
			len(entry.Extensions) > remaining-len(entry.Data) ||
			!matchesSealedLogEntryV1(entry, proof) {
			return sealedLogHistoryV1{}, fmt.Errorf(
				"%w: log entry %d", errSealedInitialization, index,
			)
		}
		previousTerm = entry.Term
		remaining -= len(entry.Data) + len(entry.Extensions)
		history.entries = append(history.entries, cloneSealedLog(entry))
		if index == last {
			return history, nil
		}
	}
}

func matchesSealedLogEntryV1(entry Log, proof sealedLogProofV1) bool {
	if entry.Index == proof.tailIndex &&
		(entry.Type != LogCommand || entry.Term != proof.tailTerm ||
			sha256.Sum256(entry.Data) != proof.tailDigest) {
		return false
	}
	switch entry.Type {
	case LogCommand:
		return len(entry.Data) > 0
	case LogNoop, LogBarrier:
		return len(entry.Data) == 0 && entry.Index != proof.tailIndex
	case LogConfiguration:
		if len(entry.Data) == 0 || entry.Index == proof.tailIndex {
			return false
		}
		var configuration Configuration
		return decodeMsgPack(entry.Data, &configuration) == nil &&
			checkSealedConfiguration(configuration) == nil &&
			bytes.Equal(entry.Data, EncodeConfiguration(configuration))
	default:
		return false
	}
}

func replaySealedLogHistoryV1(
	ctx context.Context,
	runtime *Raft,
	proof sealedLogProofV1,
	history sealedLogHistoryV1,
	electionTerm uint64,
	confirmApply func(Log, interface{}) error,
) (sealedObservation, error) {
	var committed, latest Configuration
	var committedIndex, latestIndex uint64
	for _, entry := range history.entries {
		if err := ctx.Err(); err != nil {
			return sealedObservation{}, errors.Join(
				errSealedInitialization, err,
			)
		}
		if entry.Type == LogConfiguration {
			var next Configuration
			if err := decodeMsgPack(entry.Data, &next); err != nil {
				return sealedObservation{}, errors.Join(
					errSealedInitialization, err,
				)
			}
			if err := checkSealedConfiguration(next); err != nil ||
				!bytes.Equal(entry.Data, EncodeConfiguration(next)) {
				return sealedObservation{}, errors.Join(
					errSealedInitialization, err,
				)
			}
			latest, latestIndex = next.Clone(), entry.Index
			if entry.Index <= proof.tailIndex {
				committed, committedIndex = next.Clone(), entry.Index
			}
		}
		if entry.Index <= proof.tailIndex && entry.Type == LogCommand {
			toApply := cloneSealedLog(entry)
			result := runtime.fsm.Apply(&toApply)
			if err := ctx.Err(); err != nil {
				return sealedObservation{}, errors.Join(
					errSealedInitialization, err,
				)
			}
			if err := confirmApply(cloneSealedLog(entry), result); err != nil {
				return sealedObservation{}, errors.Join(
					errSealedInitialization, err,
				)
			}
		}
	}
	if committedIndex == 0 || latestIndex == 0 {
		return sealedObservation{}, errSealedInitialization
	}
	last := history.entries[len(history.entries)-1]
	runtime.raftState.setState(Follower)
	runtime.raftState.setCurrentTerm(electionTerm) // in-memory only
	runtime.setLastSnapshot(0, 0)
	runtime.setLastLog(last.Index, last.Term)
	runtime.setCommittedConfiguration(committed, committedIndex)
	runtime.setLatestConfiguration(latest, latestIndex)
	runtime.setCommitIndex(proof.tailIndex)
	runtime.setLastApplied(proof.tailIndex)
	return observeSealedRuntimeV1(runtime, proof.tailTerm), nil
}

func checkEmptySealedSnapshotsV1(snapshots sealedSnapshotReader) error {
	list, err := snapshots.List()
	if err != nil || len(list) != 0 {
		return errors.Join(errSealedInitialization, err)
	}
	return nil
}

func refenceSealedLogSourcesV1(
	ctx context.Context,
	proof sealedLogProofV1,
	history sealedLogHistoryV1,
	election sealedElection,
	logs sealedLogReader,
	stable sealedStableReader,
	snapshots sealedSnapshotReader,
) error {
	gotHistory, err := captureSealedLogHistoryV1(
		ctx, logs, proof, election.term,
	)
	if err != nil || !reflect.DeepEqual(history, gotHistory) {
		return errors.Join(errSealedInitialization, err)
	}
	if err := checkEmptySealedSnapshotsV1(snapshots); err != nil {
		return err
	}
	gotElection, err := readSealedElection(stable)
	if err != nil || election != gotElection || ctx.Err() != nil {
		return errors.Join(errSealedInitialization, err, ctx.Err())
	}
	return nil
}
