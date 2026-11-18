// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
)

// The initialization kernel receives no start capability, transport,
// writable store or background worker.
var errSealedInitialization = errors.New("sealed Raft initialization refused")

const (
	sealedEntryLimit = 4096
	sealedLogBytes   = 32 << 20
	sealedSnapBytes  = 32 << 20
)

type sealedLogReader interface {
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(uint64, *Log) error
}

type sealedStableReader interface {
	Get([]byte) ([]byte, error)
}

type sealedSnapshotReader interface {
	List() ([]*SnapshotMeta, error)
	Open(string) (*SnapshotMeta, io.ReadCloser, error)
}

// A production adapter must acquire these facts from its original verified
// owner. This prototype does not mint an applied checkpoint or accept RPCs.
type sealedProof struct {
	snapshot   SnapshotMeta
	bodyDigest [sha256.Size]byte
	tailIndex  uint64
	tailTerm   uint64
	tailDigest [sha256.Size]byte
}

type sealedApplication interface {
	Restore(io.ReadCloser) error
	Apply(*Log) interface{}
}

type sealedObservation struct {
	CommitIndex, AppliedIndex, SnapshotIndex, LastLogIndex uint64
	TailTerm, ElectionTerm, ConfigurationIndex             uint64
	Configuration                                          Configuration
}

// The concrete Raft and application are retained, not returned to consumers
// as live interfaces. Even inside the library this value has no start path.
type sealedInitialization struct {
	raft        *Raft
	app         sealedApplication
	observation sealedObservation
	startup     context.Context
	revalidate  func() error
	workerMu    sync.Mutex
	workerClaim bool
}

func initializeSealedRaft(ctx context.Context, proof sealedProof, logs sealedLogReader,
	stable sealedStableReader, snapshots sealedSnapshotReader, app sealedApplication,
	confirmApply func(Log, interface{}) error, confirmState func(sealedObservation) error,
) (*sealedInitialization, error) {
	if app == nil {
		return nil, errSealedInitialization
	}
	sealed, err := initializeSealedRaftIntoV1(ctx, &Raft{fsm: appAdapter{app}}, proof,
		logs, stable, snapshots, confirmApply, confirmState)
	if err != nil {
		return nil, err
	}
	// Keep the original V1 application's retained identity, not its adapter.
	sealed.app = app
	// The legacy readback-only result must not retain a startup capability or
	// extend custody of the captured history through a new revalidation closure.
	sealed.startup = nil
	sealed.revalidate = nil
	return sealed, nil
}

// initializeSealedRaftIntoV1 applies the original proof to the same private
// runtime assembled by its owner. It never rebuilds that runtime or starts its
// workers. The owner must retain the original application and closed transport.
func initializeSealedRaftIntoV1(ctx context.Context, runtime *Raft, proof sealedProof,
	logs sealedLogReader, stable sealedStableReader, snapshots sealedSnapshotReader,
	confirmApply func(Log, interface{}) error, confirmState func(sealedObservation) error,
) (*sealedInitialization, error) {
	if ctx == nil || ctx.Err() != nil || logs == nil || stable == nil || snapshots == nil ||
		runtime == nil || runtime.fsm == nil || confirmApply == nil || confirmState == nil {
		return nil, errSealedInitialization
	}
	app := runtime.fsm
	if _, ok := ctx.Deadline(); !ok {
		return nil, errSealedInitialization
	}
	if proof.snapshot.ID == "" || proof.snapshot.Version != 1 || proof.snapshot.Size <= 0 ||
		proof.snapshot.Size > sealedSnapBytes || proof.snapshot.Index == 0 || proof.snapshot.Term == 0 ||
		proof.snapshot.ConfigurationIndex == 0 || proof.snapshot.ConfigurationIndex > proof.snapshot.Index ||
		proof.tailIndex <= proof.snapshot.Index || proof.tailTerm < proof.snapshot.Term {
		return nil, errSealedInitialization
	}
	if err := checkSealedConfiguration(proof.snapshot.Configuration); err != nil {
		return nil, errors.Join(errSealedInitialization, err)
	}
	proof.snapshot.Configuration = proof.snapshot.Configuration.Clone()
	proof.snapshot.Peers = bytes.Clone(proof.snapshot.Peers)
	election, err := readSealedElection(stable)
	if err != nil {
		return nil, err
	}
	history, err := captureSealedHistory(ctx, logs, proof, election.term)
	if err != nil {
		return nil, err
	}
	body, err := readSealedSnapshot(snapshots, proof)
	if err != nil || ctx.Err() != nil {
		return nil, errors.Join(errSealedInitialization, err, ctx.Err())
	}
	if restoreErr := app.Restore(io.NopCloser(bytes.NewReader(body))); restoreErr != nil {
		return nil, errors.Join(errSealedInitialization, restoreErr)
	}
	r, err := replaySealedHistory(ctx, runtime, proof, history, election.term, confirmApply)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(errSealedInitialization, err)
	}
	observation := observeSealedRuntimeV1(r, proof.tailTerm)
	confirm := observation
	confirm.Configuration = observation.Configuration.Clone()
	if err := confirmState(confirm); err != nil {
		return nil, errors.Join(errSealedInitialization, err)
	}
	if err := refenceSealedSources(ctx, proof, history, election, logs, stable, snapshots); err != nil {
		return nil, err
	}
	originalFSM, originalLogs, originalStable := r.fsm, r.logs, r.stable
	originalSnapshots, originalTransport := r.snapshots, r.trans
	originalSnapshotIndex, originalSnapshotTerm := r.getLastSnapshot()
	originalLogIndex, originalLogTerm := r.getLastLog()
	originalConfig := r.conf.Load()
	originalLocalID, originalLocalAddr, originalProtocol := r.localID, r.localAddr, r.protocolVersion
	verifyRuntime := func() bool {
		snapshotIndex, snapshotTerm := r.getLastSnapshot()
		logIndex, logTerm := r.getLastLog()
		return r.State() == Follower &&
			r.localID == originalLocalID && r.localAddr == originalLocalAddr && r.protocolVersion == originalProtocol &&
			snapshotIndex == originalSnapshotIndex && snapshotTerm == originalSnapshotTerm &&
			logIndex == originalLogIndex && logTerm == originalLogTerm &&
			reflect.DeepEqual(r.conf.Load(), originalConfig) &&
			reflect.DeepEqual(observeSealedRuntimeV1(r, proof.tailTerm), observation) &&
			r.configurations.latestIndex == observation.ConfigurationIndex &&
			reflect.DeepEqual(r.configurations.latest, observation.Configuration) &&
			sameSealedInputV1(r.fsm, originalFSM) && sameSealedInputV1(r.logs, originalLogs) &&
			sameSealedInputV1(r.stable, originalStable) && sameSealedInputV1(r.snapshots, originalSnapshots) &&
			sameSealedInputV1(r.trans, originalTransport)
	}
	retained := &sealedInitialization{
		raft: r, app: app, observation: observation, startup: ctx,
	}
	retained.revalidate = func() error {
		check := ctx
		if retained.raft != r || !sameSealedInputV1(retained.startup, ctx) || !verifyRuntime() {
			return errSealedInitialization
		}
		if err := check.Err(); err != nil {
			return errors.Join(errSealedInitialization, err)
		}
		if err := refenceSealedSources(check, proof, history, election, logs, stable, snapshots); err != nil {
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
		return refenceSealedSources(check, proof, history, election, logs, stable, snapshots)
	}
	return retained, nil
}

func observeSealedRuntimeV1(runtime *Raft, tailTerm uint64) sealedObservation {
	snapshotIndex, _ := runtime.getLastSnapshot()
	return sealedObservation{
		CommitIndex: runtime.CommitIndex(), AppliedIndex: runtime.AppliedIndex(),
		SnapshotIndex: snapshotIndex, LastLogIndex: runtime.LastIndex(),
		TailTerm: tailTerm, ElectionTerm: runtime.CurrentTerm(),
		ConfigurationIndex: runtime.configurations.committedIndex,
		Configuration:      runtime.configurations.committed.Clone(),
	}
}

func sameSealedInputV1(left, right interface{}) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if !reflect.ValueOf(left).Comparable() || !reflect.ValueOf(right).Comparable() {
		return false
	}
	return left == right
}

func checkSealedConfiguration(configuration Configuration) error {
	for _, server := range configuration.Servers {
		switch server.Suffrage {
		case Voter, Nonvoter, Staging:
		default:
			return fmt.Errorf("unknown server suffrage: %d", server.Suffrage)
		}
	}
	return checkConfiguration(configuration)
}

type sealedElection struct {
	term, voteTerm uint64
	vote           [sha256.Size]byte
}

func readSealedElection(stable sealedStableReader) (sealedElection, error) {
	var value sealedElection
	raw, err := stable.Get(keyCurrentTerm)
	if err != nil || len(raw) != 8 {
		return value, errors.Join(errSealedInitialization, err)
	}
	value.term = binary.BigEndian.Uint64(raw)
	if value.term == 0 {
		return value, errSealedInitialization
	}
	voteTerm, termErr := stable.Get(keyLastVoteTerm)
	voter, voteErr := stable.Get(keyLastVoteCand)
	if termErr != nil && termErr.Error() == "not found" && voteErr != nil && voteErr.Error() == "not found" {
		return value, nil
	}
	if termErr != nil || voteErr != nil || len(voteTerm) != 8 || len(voter) == 0 || len(voter) > 512 {
		return value, errors.Join(errSealedInitialization, termErr, voteErr)
	}
	value.voteTerm, value.vote = binary.BigEndian.Uint64(voteTerm), sha256.Sum256(voter)
	if value.voteTerm == 0 || value.voteTerm > value.term {
		return value, errSealedInitialization
	}
	return value, nil
}

type sealedHistory struct {
	first, last uint64
	entries     []Log // snapshot boundary through physical end, inclusive
}

func captureSealedHistory(ctx context.Context, logs sealedLogReader, proof sealedProof, term uint64) (sealedHistory, error) {
	first, firstErr := logs.FirstIndex()
	last, lastErr := logs.LastIndex()
	if firstErr != nil || lastErr != nil || first == 0 || first > proof.snapshot.Index ||
		last < proof.tailIndex || last-proof.snapshot.Index > sealedEntryLimit {
		return sealedHistory{}, errors.Join(errSealedInitialization, firstErr, lastErr)
	}
	history := sealedHistory{first: first, last: last}
	previousTerm, remaining := proof.snapshot.Term, sealedLogBytes
	for index := proof.snapshot.Index; ; index++ {
		if err := ctx.Err(); err != nil {
			return sealedHistory{}, errors.Join(errSealedInitialization, err)
		}
		var entry Log
		if err := logs.GetLog(index, &entry); err != nil {
			return sealedHistory{}, errors.Join(errSealedInitialization, err)
		}
		if entry.Index != index || entry.Term < previousTerm || entry.Term > term ||
			len(entry.Data) > remaining || len(entry.Extensions) > remaining-len(entry.Data) ||
			!matchesSealedEntry(entry, proof) {
			return sealedHistory{}, fmt.Errorf("%w: log entry %d", errSealedInitialization, index)
		}
		previousTerm = entry.Term
		remaining -= len(entry.Data) + len(entry.Extensions)
		history.entries = append(history.entries, cloneSealedLog(entry))
		if index == last {
			return history, nil
		}
	}
}

func matchesSealedEntry(entry Log, proof sealedProof) bool {
	switch entry.Index {
	case proof.snapshot.Index:
		return entry.Term == proof.snapshot.Term
	case proof.tailIndex:
		return entry.Type == LogCommand && entry.Term == proof.tailTerm &&
			sha256.Sum256(entry.Data) == proof.tailDigest
	default:
		switch entry.Type {
		case LogCommand:
			return len(entry.Data) > 0
		case LogNoop, LogBarrier:
			return len(entry.Data) == 0
		case LogConfiguration:
			return entry.Index < proof.tailIndex
		}
		return false
	}
}

func cloneSealedLog(entry Log) Log {
	entry.Data, entry.Extensions = bytes.Clone(entry.Data), bytes.Clone(entry.Extensions)
	return entry
}

func readSealedSnapshot(snapshots sealedSnapshotReader, proof sealedProof) ([]byte, error) {
	list, err := snapshots.List()
	if err != nil || len(list) != 1 || !reflect.DeepEqual(list[0], &proof.snapshot) {
		return nil, errors.Join(errSealedInitialization, err)
	}
	meta, body, err := snapshots.Open(proof.snapshot.ID)
	if err != nil || body == nil {
		if body != nil {
			err = errors.Join(err, body.Close())
		}
		return nil, errors.Join(errSealedInitialization, err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(body, proof.snapshot.Size+1))
	closeErr := body.Close()
	if err := errors.Join(readErr, closeErr); err != nil || !reflect.DeepEqual(meta, &proof.snapshot) ||
		int64(len(raw)) != proof.snapshot.Size || sha256.Sum256(raw) != proof.bodyDigest {
		return nil, errors.Join(errSealedInitialization, err)
	}
	return raw, nil
}

func replaySealedHistory(ctx context.Context, r *Raft, proof sealedProof, history sealedHistory,
	electionTerm uint64, confirmApply func(Log, interface{}) error,
) (*Raft, error) {
	app := r.fsm
	config, configIndex := proof.snapshot.Configuration, proof.snapshot.ConfigurationIndex
	for _, entry := range history.entries[1:] {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(errSealedInitialization, err)
		}
		if entry.Index > proof.tailIndex {
			break
		}
		switch entry.Type {
		case LogCommand:
			toApply := cloneSealedLog(entry)
			result := app.Apply(&toApply)
			if err := ctx.Err(); err != nil {
				return nil, errors.Join(errSealedInitialization, err)
			}
			if err := confirmApply(cloneSealedLog(entry), result); err != nil {
				return nil, errors.Join(errSealedInitialization, err)
			}
		case LogConfiguration:
			var next Configuration
			if err := decodeMsgPack(entry.Data, &next); err != nil {
				return nil, errors.Join(errSealedInitialization, err)
			}
			if err := checkSealedConfiguration(next); err != nil || !bytes.Equal(entry.Data, EncodeConfiguration(next)) {
				return nil, errors.Join(errSealedInitialization, err)
			}
			config, configIndex = next, entry.Index
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(errSealedInitialization, err)
	}
	last := history.entries[len(history.entries)-1]
	r.raftState.setState(Follower)
	r.raftState.setCurrentTerm(electionTerm) // in-memory only
	r.setLastSnapshot(proof.snapshot.Index, proof.snapshot.Term)
	r.setLastLog(last.Index, last.Term)
	r.setCommittedConfiguration(config, configIndex)
	r.setLatestConfiguration(config, configIndex)
	r.setCommitIndex(proof.tailIndex)
	r.setLastApplied(proof.tailIndex)
	return r, nil
}

// Never delegate snapshot creation through a staged application.
type appAdapter struct{ sealedApplication }

func (appAdapter) Snapshot() (FSMSnapshot, error) { return nil, errSealedInitialization }

func refenceSealedSources(ctx context.Context, proof sealedProof, history sealedHistory, election sealedElection,
	logs sealedLogReader, stable sealedStableReader, snapshots sealedSnapshotReader,
) error {
	gotHistory, err := captureSealedHistory(ctx, logs, proof, election.term)
	if err != nil || !reflect.DeepEqual(history, gotHistory) {
		return errors.Join(errSealedInitialization, err)
	}
	if _, snapshotErr := readSealedSnapshot(snapshots, proof); snapshotErr != nil {
		return snapshotErr
	}
	gotElection, err := readSealedElection(stable)
	if err != nil || election != gotElection || ctx.Err() != nil {
		return errors.Join(errSealedInitialization, err, ctx.Err())
	}
	return nil
}
