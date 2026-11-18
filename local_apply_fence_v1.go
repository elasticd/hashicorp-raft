// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"errors"
	"time"
)

// ErrLocalApplyFenceV1 means a local fenced command was refused before it
// received an index or reached the log store. It grants no retry authority
// for a different command, leader, configuration, or original attempt.
var ErrLocalApplyFenceV1 = errors.New("local apply fence is not current")

// localApplyFenceV1 is transient leader-loop input, never part of a log or
// peer message. The context retains the original caller's bounded lifetime.
type localApplyFenceV1 struct {
	ctx         context.Context
	deadline    time.Time
	term        uint64
	configIndex uint64
}

// ApplyWithLocalFenceV1 submits a copied command only if the event loop
// still has the expected nonzero term and identical committed/latest
// configuration index, consisting entirely of voters including this node.
// EnableLocalApplyFenceV1 must have been set before constructing this Raft.
// The supplied context must retain the original attempt's live deadline.
// Each queued command is fenced immediately before its index assignment.
//
// The returned future retains normal Apply completion semantics. After a
// request is queued, cancellation may still race with event-loop admission;
// cancellation or shutdown alone is not proof that no write occurred. Only
// ErrLocalApplyFenceV1 proves refusal before indexing. Callers must preserve
// their existing indeterminate-outcome and recovery rules. No new goroutine,
// transport authorization, wire field, or durable record is created here.
func (r *Raft) ApplyWithLocalFenceV1(ctx context.Context, command []byte, term, configurationIndex uint64) ApplyFuture {
	if r == nil || !r.localApplyFenceV1 || ctx == nil || len(command) == 0 || term == 0 || configurationIndex == 0 {
		return errorFuture{ErrLocalApplyFenceV1}
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || ctx.Err() != nil || !time.Now().Before(deadline) {
		return errorFuture{ErrLocalApplyFenceV1}
	}
	future := &logFuture{
		log: Log{Type: LogCommand, Data: bytes.Clone(command)},
		localFence: &localApplyFenceV1{
			ctx: ctx, deadline: deadline, term: term, configIndex: configurationIndex,
		},
	}
	future.init()
	future.ShutdownCh = r.shutdownCh
	select {
	case <-ctx.Done():
		// This branch proves the send did not happen. Once the send wins,
		// only the retained future can describe the queued request.
		return errorFuture{ErrLocalApplyFenceV1}
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.applyCh <- future:
		return future
	}
}

// matchesLocalApplyFenceV1 runs on the event loop immediately before one
// entry's index assignment; constructor opt-in excludes off-loop heartbeats.
func (r *Raft) matchesLocalApplyFenceV1(fence *localApplyFenceV1) bool {
	if !r.localApplyFenceV1 || fence.term == 0 || fence.configIndex == 0 ||
		fence.ctx.Err() != nil || !time.Now().Before(fence.deadline) ||
		r.State() != Leader || r.getCurrentTerm() != fence.term ||
		r.configurations.latestIndex != fence.configIndex ||
		r.configurations.committedIndex != fence.configIndex ||
		r.getCommitIndex() < fence.configIndex {
		return false
	}
	latest := r.configurations.latest.Servers
	committed := r.configurations.committed.Servers
	if len(latest) != len(committed) {
		return false
	}
	local := false
	for index, server := range latest {
		if server.Suffrage != Voter || server != committed[index] {
			return false
		}
		local = local || server.ID == r.localID
	}
	return local
}
