// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSealedRevalidationV1RefusesRuntimeTermDrift(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"snapshot", "log"} {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			sealed, _ := newSealedRevalidationFixtureV1(t, ctx)
			if err := sealed.revalidate(); err != nil {
				t.Fatalf("baseline revalidation: %v", err)
			}
			if name == "snapshot" {
				index, term := sealed.raft.getLastSnapshot()
				sealed.raft.setLastSnapshot(index, term+1)
			} else {
				index, term := sealed.raft.getLastLog()
				sealed.raft.setLastLog(index, term+1)
			}
			if err := sealed.revalidate(); !errors.Is(err, errSealedInitialization) {
				t.Fatalf("term-only runtime drift accepted: %v", err)
			}
		})
	}
}

func TestSealedRevalidationV1RefusesRetainedDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*sealedInitialization, *sealedFixture)
	}{
		{"durable term", func(_ *sealedInitialization, source *sealedFixture) {
			source.stable["CurrentTerm"][7]++
		}},
		{"durable vote term", func(_ *sealedInitialization, source *sealedFixture) {
			source.stable["LastVoteTerm"][7]++
		}},
		{"durable voter", func(_ *sealedInitialization, source *sealedFixture) {
			source.stable["LastVoteCand"] = []byte("different-voter")
		}},
		{"snapshot metadata", func(_ *sealedInitialization, source *sealedFixture) {
			source.meta.Size++
		}},
		{"snapshot body", func(_ *sealedInitialization, source *sealedFixture) {
			source.body[0]++
		}},
		{"tail command", func(_ *sealedInitialization, source *sealedFixture) {
			entry := source.logs[10]
			entry.Data = []byte("changed tail")
			source.logs[10] = entry
		}},
		{"unproved suffix", func(_ *sealedInitialization, source *sealedFixture) {
			entry := source.logs[11]
			entry.Extensions = []byte{43}
			source.logs[11] = entry
		}},
		{"runtime FSM", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.fsm = appAdapter{&sealedTestApplication{}}
		}},
		{"runtime local identity", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.localID = "different-node"
		}},
		{"runtime local address", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.localAddr = "different-address"
		}},
		{"runtime protocol", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.protocolVersion++
		}},
		{"runtime logs", func(sealed *sealedInitialization, source *sealedFixture) {
			sealed.raft.logs = &runtimeAssemblySealedStoreV1{sealedFixture: source}
		}},
		{"runtime stable", func(sealed *sealedInitialization, source *sealedFixture) {
			sealed.raft.stable = &runtimeAssemblySealedStoreV1{sealedFixture: source}
		}},
		{"runtime snapshots", func(sealed *sealedInitialization, source *sealedFixture) {
			sealed.raft.snapshots = &runtimeAssemblySealedSnapshotsV1{sealedFixture: source}
		}},
		{"runtime transport", func(sealed *sealedInitialization, _ *sealedFixture) {
			_, transport := NewInmemTransport("replacement")
			sealed.raft.trans = transport
		}},
		{"runtime state", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.setState(Candidate)
		}},
		{"runtime election term", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.raftState.setCurrentTerm(sealed.raft.CurrentTerm() + 1)
		}},
		{"runtime config", func(sealed *sealedInitialization, _ *sealedFixture) {
			config := sealed.raft.config()
			config.CommitTimeout += time.Millisecond
			sealed.raft.conf.Store(config)
		}},
		{"runtime commit", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.setCommitIndex(sealed.raft.CommitIndex() + 1)
		}},
		{"runtime applied", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.setLastApplied(sealed.raft.AppliedIndex() + 1)
		}},
		{"runtime snapshot index", func(sealed *sealedInitialization, _ *sealedFixture) {
			index, term := sealed.raft.getLastSnapshot()
			sealed.raft.setLastSnapshot(index+1, term)
		}},
		{"runtime log index", func(sealed *sealedInitialization, _ *sealedFixture) {
			index, term := sealed.raft.getLastLog()
			sealed.raft.setLastLog(index+1, term)
		}},
		{"committed configuration index", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.configurations.committedIndex++
		}},
		{"committed configuration", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.configurations.committed.Servers[0].Address = "changed"
		}},
		{"latest configuration index", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.configurations.latestIndex++
		}},
		{"latest configuration", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft.configurations.latest.Servers[0].Address = "changed"
		}},
		{"sealed runtime owner", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.raft = new(Raft)
		}},
		{"sealed startup owner", func(sealed *sealedInitialization, _ *sealedFixture) {
			sealed.startup = context.Background()
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			sealed, source := newSealedRevalidationFixtureV1(t, ctx)
			test.mutate(sealed, source)
			if err := sealed.revalidate(); !errors.Is(err, errSealedInitialization) {
				t.Fatalf("retained drift accepted: %v", err)
			}
		})
	}
}

func TestSealedRevalidationV1RefusesInvalidLifetime(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	sealed, _ := newSealedRevalidationFixtureV1(t, ctx)
	cancel()
	if err := sealed.revalidate(); !errors.Is(err, errSealedInitialization) {
		t.Fatalf("cancelled original lifetime accepted: %v", err)
	}
}

func newSealedRevalidationFixtureV1(
	t *testing.T,
	ctx context.Context,
) (*sealedInitialization, *sealedFixture) {
	t.Helper()
	source, proof := newSealedFixture()
	store := &runtimeAssemblySealedStoreV1{sealedFixture: source}
	snapshots := &runtimeAssemblySealedSnapshotsV1{sealedFixture: source}
	_, inmemTransport := NewInmemTransport("old")
	transport := &runtimeAssemblyTransportV1{
		InmemTransport: inmemTransport,
	}
	app := &runtimeAssemblySealedApplicationV1{
		sealedTestApplication: &sealedTestApplication{},
	}
	config := DefaultConfig()
	config.LocalID = "a"
	runtime, err := newRaftRuntimeV1(
		config, appAdapter{app}, store, store, snapshots, transport,
	)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := initializeSealedRaftIntoV1(
		ctx,
		runtime,
		proof,
		store,
		store,
		snapshots,
		confirmSealedFixtureApply,
		func(observation sealedObservation) error {
			return confirmSealedFixtureState(
				app.sealedTestApplication,
				observation,
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return sealed, source
}
