// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

type sealedFixture struct {
	meta   SnapshotMeta
	body   []byte
	last   uint64
	logs   map[uint64]Log
	stable map[string][]byte
}

func (s *sealedFixture) List() ([]*SnapshotMeta, error) { return []*SnapshotMeta{&s.meta}, nil }
func (s *sealedFixture) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	if id != s.meta.ID {
		return nil, nil, errors.New("wrong snapshot")
	}
	return &s.meta, io.NopCloser(bytes.NewReader(s.body)), nil
}
func (*sealedFixture) FirstIndex() (uint64, error)  { return 7, nil }
func (s *sealedFixture) LastIndex() (uint64, error) { return s.last, nil }
func (s *sealedFixture) GetLog(index uint64, entry *Log) error {
	if log, ok := s.logs[index]; ok {
		*entry = cloneSealedLog(log)
		return nil
	}
	return ErrLogNotFound
}
func (s *sealedFixture) Get(key []byte) ([]byte, error) {
	if value, ok := s.stable[string(key)]; ok {
		return bytes.Clone(value), nil
	}
	return nil, errors.New("not found")
}

type sealedTestApplication struct {
	data  []byte
	calls []uint64
	after func()
}

func (app *sealedTestApplication) Restore(r io.ReadCloser) error {
	data, err := io.ReadAll(r)
	app.data = data
	return errors.Join(err, r.Close())
}

func (app *sealedTestApplication) Apply(log *Log) interface{} {
	app.calls = append(app.calls, log.Index)
	app.data = append(app.data, log.Data...)
	if app.after != nil {
		app.after()
	}
	return log.Index
}

func newSealedFixture() (*sealedFixture, sealedProof) {
	before := Configuration{Servers: []Server{{ID: "a", Address: "old", Suffrage: Voter}}}
	after := Configuration{Servers: []Server{{ID: "a", Address: "new", Suffrage: Voter}}}
	source := &sealedFixture{
		meta: SnapshotMeta{ID: "fixed", Version: 1, Index: 8, Term: 3,
			Configuration: before, ConfigurationIndex: 2, Size: 3},
		body: []byte("old"), last: 11,
		logs: map[uint64]Log{
			7:  {Index: 7, Term: 3, Type: LogNoop},
			8:  {Index: 8, Term: 3, Type: LogNoop},
			9:  {Index: 9, Term: 4, Type: LogConfiguration, Data: EncodeConfiguration(after)},
			10: {Index: 10, Term: 4, Type: LogCommand, Data: []byte(" tail")},
			11: {Index: 11, Term: 4, Type: LogCommand, Data: []byte(" unproved"), Extensions: []byte{42}},
		},
		stable: map[string][]byte{
			"CurrentTerm":  binary.BigEndian.AppendUint64(nil, 6),
			"LastVoteTerm": binary.BigEndian.AppendUint64(nil, 5),
			"LastVoteCand": []byte("prior-vote"),
		},
	}
	proof := sealedProof{snapshot: source.meta, bodyDigest: sha256.Sum256(source.body),
		tailIndex: 10, tailTerm: 4, tailDigest: sha256.Sum256(source.logs[10].Data)}
	proof.snapshot.Configuration = source.meta.Configuration.Clone()
	return source, proof
}

func confirmSealedFixtureApply(log Log, result interface{}) error {
	if position, ok := result.(uint64); !ok || position != log.Index {
		return errors.New("application did not confirm the command position")
	}
	return nil
}

func confirmSealedFixtureState(app *sealedTestApplication, facts sealedObservation) error {
	if string(app.data) != "old tail" || !reflect.DeepEqual(app.calls, []uint64{10}) ||
		facts.CommitIndex != 10 || facts.AppliedIndex != 10 || facts.SnapshotIndex != 8 ||
		facts.LastLogIndex != 11 || facts.TailTerm != 4 || facts.ElectionTerm != 6 ||
		facts.ConfigurationIndex != 9 || len(facts.Configuration.Servers) != 1 ||
		facts.Configuration.Servers[0].Address != "new" {
		return errors.New("wrong independently observed application or Raft boundary")
	}
	return nil
}

func TestSealedRaftInitializationKeepsOriginalSources(t *testing.T) {
	t.Parallel()
	source, proof := newSealedFixture()
	expected, _ := newSealedFixture()
	for repeat := 0; repeat < 2; repeat++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		app := &sealedTestApplication{}
		got, err := initializeSealedRaft(ctx, proof, source, source, source, app, confirmSealedFixtureApply,
			func(facts sealedObservation) error { return confirmSealedFixtureState(app, facts) })
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.app != app || got.raft.trans != nil || got.raft.rpcCh != nil ||
			got.raft.stable != nil || got.raft.logs != nil || got.raft.snapshots != nil ||
			got.raft.applyCh != nil || got.raft.shutdownCh != nil || got.raft.State() != Follower {
			t.Fatal("sequenced local replay produced runnable Raft or transport access")
		}
		if !reflect.DeepEqual(source, expected) {
			t.Fatal("the original snapshot, logs or election records changed")
		}
	}
}

func TestSealedLocalRaftV1ReturnsDetachedReadback(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	source, proof := newSealedFixture()
	app := &sealedTestApplication{}
	checkpoint := SealedCheckpointV1{
		Snapshot: proof.snapshot, BodySHA256: proof.bodyDigest,
		TailIndex: proof.tailIndex, TailTerm: proof.tailTerm, TailSHA256: proof.tailDigest,
	}
	got, err := NewSealedLocalRaftV1(ctx, checkpoint, source, source, source, appAdapter{app},
		confirmSealedFixtureApply, func(observation SealedReadbackV1) error {
			err := confirmSealedFixtureState(app, observation)
			observation.Configuration.Servers[0].Address = "outside callback"
			return err
		})
	if err != nil || got == nil {
		t.Fatalf("initialize sealed Raft: %v", err)
	}
	facts, ok := got.Readback()
	if !ok || confirmSealedFixtureState(app, facts) != nil {
		t.Fatal("callback modified retained readback")
	}
	facts.Configuration.Servers[0].Address = "outside reader"
	facts, ok = got.Readback()
	if !ok || confirmSealedFixtureState(app, facts) != nil {
		t.Fatal("reader modified retained readback")
	}
	uninitialized := new(SealedLocalRaftV1)
	if _, ok := uninitialized.Readback(); ok {
		t.Fatal("zero sealed value reported verified readback")
	}
}

func TestSealedRaftInitializationRefusesWithoutExposingState(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"missing boundary", "changed boundary", "wrong command", "tail gap",
		"term behind suffix", "incomplete vote", "malformed term", "later configuration",
		"bad snapshot", "application rejected", "final readback rejected", "later entry drift",
		"vote drift", "snapshot drift", "cancelled during replay",
	} {
		t.Run(name, func(t *testing.T) {
			source, proof := newSealedFixture()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			app := &sealedTestApplication{}
			mutateSealedFixture(name, source, app, cancel)
			checkApply := confirmSealedFixtureApply
			if name == "application rejected" {
				checkApply = func(Log, interface{}) error { return errors.New("rejected") }
			}
			got, err := initializeSealedRaft(ctx, proof, source, source, source, app, checkApply,
				func(facts sealedObservation) error {
					if name == "final readback rejected" {
						return errors.New("different application proof")
					}
					return confirmSealedFixtureState(app, facts)
				})
			if got != nil || !errors.Is(err, errSealedInitialization) {
				t.Fatalf("case %q exposed an initialization: %v", name, err)
			}
		})
	}
}

func mutateSealedFixture(name string, source *sealedFixture, app *sealedTestApplication, cancel context.CancelFunc) {
	switch name {
	case "missing boundary":
		delete(source.logs, 8)
	case "changed boundary":
		source.logs[8] = Log{Index: 8, Term: 4, Type: LogNoop}
	case "wrong command":
		source.logs[10] = Log{Index: 10, Term: 4, Type: LogCommand, Data: []byte("different")}
	case "tail gap":
		delete(source.logs, 9)
	case "term behind suffix":
		source.stable["CurrentTerm"] = binary.BigEndian.AppendUint64(nil, 3)
	case "incomplete vote":
		delete(source.stable, "LastVoteCand")
	case "malformed term":
		source.stable["CurrentTerm"] = []byte{6}
	case "later configuration":
		source.logs[11] = Log{Index: 11, Term: 4, Type: LogConfiguration, Data: source.logs[9].Data}
	case "bad snapshot":
		source.body[0] ^= 1
	case "later entry drift":
		app.after = func() {
			entry := source.logs[11]
			entry.Extensions = []byte{43}
			source.logs[11] = entry
		}
	case "vote drift":
		app.after = func() { source.stable["LastVoteCand"] = []byte("changed-vote") }
	case "snapshot drift":
		app.after = func() { source.body[0] ^= 1 }
	case "cancelled during replay":
		app.after = cancel
	}
}
