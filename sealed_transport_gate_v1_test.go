// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These spies never open sockets. The source consumer is deliberately
// unbuffered so that accepting an RPC is observable independently of delivery.
type sealedGateSpyV1 struct {
	mu           sync.Mutex
	consumer     chan RPC
	addr         ServerAddress
	calls        map[string]int
	err          error
	closeErr     error
	closeEntered chan struct{}
	closeProceed chan struct{}
	pipeline     AppendPipeline
	pipelineErr  error
}

func newSealedGateSpyV1() *sealedGateSpyV1 {
	return &sealedGateSpyV1{consumer: make(chan RPC), addr: "sealed-local", calls: make(map[string]int), err: errors.New("source RPC error")}
}

func (s *sealedGateSpyV1) record(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[name]++
	return s.err
}

func (s *sealedGateSpyV1) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[name]
}

func (s *sealedGateSpyV1) Consumer() <-chan RPC { s.record("consumer"); return s.consumer }
func (s *sealedGateSpyV1) LocalAddr() ServerAddress {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["addr"]++
	return s.addr
}
func (s *sealedGateSpyV1) AppendEntriesPipeline(ServerID, ServerAddress) (AppendPipeline, error) {
	err := s.record("pipeline")
	if s.pipeline != nil {
		return s.pipeline, s.pipelineErr
	}
	return nil, err
}
func (s *sealedGateSpyV1) AppendEntries(ServerID, ServerAddress, *AppendEntriesRequest, *AppendEntriesResponse) error {
	return s.record("append")
}
func (s *sealedGateSpyV1) RequestVote(ServerID, ServerAddress, *RequestVoteRequest, *RequestVoteResponse) error {
	return s.record("vote")
}
func (s *sealedGateSpyV1) InstallSnapshot(ServerID, ServerAddress, *InstallSnapshotRequest, *InstallSnapshotResponse, io.Reader) error {
	return s.record("snapshot")
}
func (s *sealedGateSpyV1) EncodePeer(_ ServerID, addr ServerAddress) []byte { return []byte(addr) }
func (s *sealedGateSpyV1) DecodePeer(value []byte) ServerAddress            { return ServerAddress(value) }
func (s *sealedGateSpyV1) SetHeartbeatHandler(callback func(RPC)) {
	if callback == nil {
		s.record("heartbeat-clear")
		return
	}
	s.record("heartbeat")
}
func (s *sealedGateSpyV1) TimeoutNow(ServerID, ServerAddress, *TimeoutNowRequest, *TimeoutNowResponse) error {
	return s.record("timeout")
}

type sealedGateClosableSpyV1 struct{ *sealedGateSpyV1 }

func (s *sealedGateClosableSpyV1) Close() error {
	s.record("close")
	if s.closeEntered != nil {
		s.closeEntered <- struct{}{}
	}
	if s.closeProceed != nil {
		<-s.closeProceed
	}
	return s.closeErr
}

type sealedGatePreVoteSpyV1 struct{ *sealedGateClosableSpyV1 }

func (s *sealedGatePreVoteSpyV1) RequestPreVote(ServerID, ServerAddress, *RequestPreVoteRequest, *RequestPreVoteResponse) error {
	return s.record("prevote")
}

func sealedGateOutboundCallsV1(transport Transport) map[string]error {
	_, pipelineErr := transport.AppendEntriesPipeline("peer", "target")
	return map[string]error{
		"pipeline": pipelineErr,
		"append":   transport.AppendEntries("peer", "target", &AppendEntriesRequest{}, &AppendEntriesResponse{}),
		"vote":     transport.RequestVote("peer", "target", &RequestVoteRequest{}, &RequestVoteResponse{}),
		"snapshot": transport.InstallSnapshot("peer", "target", &InstallSnapshotRequest{}, &InstallSnapshotResponse{}, nil),
		"timeout":  transport.TimeoutNow("peer", "target", &TimeoutNowRequest{}, &TimeoutNowResponse{}),
	}
}

func requireSealedGateRefusalV1(t *testing.T, transport Transport, spy *sealedGateSpyV1) {
	t.Helper()
	for name, err := range sealedGateOutboundCallsV1(transport) {
		if !errors.Is(err, errSealedTransportGateV1) {
			t.Fatalf("%s not refused: %v", name, err)
		}
		if got := spy.count(name); got != 0 {
			t.Fatalf("%s reached source %d times", name, got)
		}
	}
}

func TestSealedTransportGateV1RefusesSourcesWithoutCustody(t *testing.T) {
	spy := newSealedGateSpyV1()
	var typedNil *sealedGateClosableSpyV1
	for _, source := range []Transport{nil, typedNil, spy} {
		transport, control, err := newSealedTransportGateV1(source)
		if transport != nil || control != nil || !errors.Is(err, errSealedTransportGateV1) {
			t.Fatalf("unsupported source admitted: transport=%T control=%v err=%v", transport, control, err)
		}
	}
	if spy.count("consumer") != 0 || spy.count("addr") != 0 {
		t.Fatal("unsupported source accessed")
	}
}

type sealedGatePipelineSpyV1 struct {
	mu                  sync.Mutex
	appends, closes     int
	consumer            chan AppendFuture
	appendErr, closeErr error
}

func (p *sealedGatePipelineSpyV1) AppendEntries(*AppendEntriesRequest, *AppendEntriesResponse) (AppendFuture, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.appends++
	return nil, p.appendErr
}
func (p *sealedGatePipelineSpyV1) Consumer() <-chan AppendFuture { return p.consumer }
func (p *sealedGatePipelineSpyV1) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closes++
	return p.closeErr
}

func TestSealedTransportGateV1OwnsPipelineCleanup(t *testing.T) {
	for _, closePipelineFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "gate-first", true: "pipeline-first"}[closePipelineFirst], func(t *testing.T) {
			spy := newSealedGateSpyV1()
			pipeline := &sealedGatePipelineSpyV1{consumer: make(chan AppendFuture), appendErr: errors.New("pipeline append"), closeErr: errors.New("pipeline close")}
			spy.pipeline = pipeline
			transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = control.Close() })
			if releaseErr := control.releaseV1(); releaseErr != nil {
				t.Fatal(releaseErr)
			}
			wrapped, err := transport.AppendEntriesPipeline("peer", "target")
			if err != nil || wrapped == nil || wrapped == pipeline {
				t.Fatalf("pipeline not privately wrapped: %T %v", wrapped, err)
			}
			if _, err := wrapped.AppendEntries(&AppendEntriesRequest{}, &AppendEntriesResponse{}); err != pipeline.appendErr {
				t.Fatalf("pipeline source error lost: %v", err)
			}
			if closePipelineFirst {
				if err := wrapped.Close(); err != pipeline.closeErr {
					t.Fatalf("pipeline close error lost: %v", err)
				}
			}
			if err := control.Close(); !errors.Is(err, pipeline.closeErr) {
				t.Fatalf("gate close lost owned pipeline failure: %v", err)
			}
			if err := wrapped.Close(); err != pipeline.closeErr {
				t.Fatalf("pipeline close error not sticky: %v", err)
			}
			if _, err := wrapped.AppendEntries(&AppendEntriesRequest{}, &AppendEntriesResponse{}); !errors.Is(err, errSealedTransportGateV1) {
				t.Fatalf("closed pipeline admitted append: %v", err)
			}
			pipeline.mu.Lock()
			appends, closes := pipeline.appends, pipeline.closes
			pipeline.mu.Unlock()
			if appends != 1 || closes != 1 || spy.count("close") != 1 {
				t.Fatalf("cleanup custody appends=%d pipeline closes=%d source closes=%d", appends, closes, spy.count("close"))
			}
		})
	}
}

func TestSealedTransportGateV1HoldsDeliveryAndOutboundUntilRelease(t *testing.T) {
	spy := newSealedGateSpyV1()
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	consumer := transport.Consumer()
	if spy.count("heartbeat-clear") != 1 {
		t.Fatal("constructor did not clear source heartbeat fast path exactly once")
	}
	if consumer == nil || consumer == spy.consumer || consumer != transport.Consumer() {
		t.Fatal("consumer is not a fixed private channel")
	}
	spy.mu.Lock()
	spy.addr = "changed-source-address"
	spy.mu.Unlock()
	if transport.LocalAddr() != "sealed-local" || spy.count("addr") != 1 {
		t.Fatal("local address was not captured exactly once")
	}
	if _, ok := transport.(WithPreVote); ok {
		t.Fatal("invented optional PreVote support")
	}
	requireSealedGateRefusalV1(t, transport, spy)
	select {
	case spy.consumer <- RPC{Command: "held"}:
		t.Fatal("held gate drained the source consumer")
	case <-time.After(20 * time.Millisecond):
	}
	transport.SetHeartbeatHandler(nil)
	if spy.count("heartbeat") != 0 {
		t.Fatal("heartbeat registration reached source")
	}
	if err := control.releaseV1(); err != nil {
		t.Fatal(err)
	}
	if err := control.releaseV1(); !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("second release: %v", err)
	}
	response := make(chan RPCResponse, 1)
	sent := make(chan struct{})
	go func() { spy.consumer <- RPC{Command: "released", RespChan: response}; close(sent) }()
	select {
	case got := <-consumer:
		if got.Command != "released" {
			t.Fatalf("wrong RPC: %#v", got)
		}
		got.Respond("reply", nil)
	case <-time.After(time.Second):
		t.Fatal("released RPC not delivered")
	}
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("source sender did not finish")
	}
	if got := <-response; got.Response != "reply" {
		t.Fatalf("response custody lost: %#v", got)
	}
	for name, err := range sealedGateOutboundCallsV1(transport) {
		if err != spy.err || spy.count(name) != 1 {
			t.Fatalf("released %s err=%v calls=%d", name, err, spy.count(name))
		}
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	for name, err := range sealedGateOutboundCallsV1(transport) {
		if !errors.Is(err, errSealedTransportGateV1) || spy.count(name) != 1 {
			t.Fatalf("closed %s err=%v calls=%d", name, err, spy.count(name))
		}
	}
}

func TestSealedTransportGateV1PreservesOptionalPreVote(t *testing.T) {
	spy := newSealedGateSpyV1()
	transport, control, err := newSealedTransportGateV1(&sealedGatePreVoteSpyV1{&sealedGateClosableSpyV1{spy}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	preVote, ok := transport.(WithPreVote)
	if !ok {
		t.Fatal("source PreVote capability erased")
	}
	invoke := func() error {
		return preVote.RequestPreVote("peer", "target", &RequestPreVoteRequest{}, &RequestPreVoteResponse{})
	}
	if err := invoke(); !errors.Is(err, errSealedTransportGateV1) || spy.count("prevote") != 0 {
		t.Fatalf("held PreVote: %v", err)
	}
	if err := control.releaseV1(); err != nil {
		t.Fatal(err)
	}
	if err := invoke(); err != spy.err || spy.count("prevote") != 1 {
		t.Fatalf("released PreVote: %v", err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if err := invoke(); !errors.Is(err, errSealedTransportGateV1) || spy.count("prevote") != 1 {
		t.Fatalf("closed PreVote: %v", err)
	}
}

func TestSealedTransportGateV1RefusesHeartbeatFastPath(t *testing.T) {
	spy := newSealedGateSpyV1()
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	transport.SetHeartbeatHandler(func(RPC) { t.Error("heartbeat callback invoked") })
	transport.SetHeartbeatHandler(nil) // Clearing must not erase the failure.
	if spy.count("heartbeat") != 0 {
		t.Fatal("heartbeat fast path reached source")
	}
	if err := control.releaseV1(); !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("poisoned release: %v", err)
	}
	requireSealedGateRefusalV1(t, transport, spy)
}

func TestSealedTransportGateV1HeartbeatFailureRevokesReleasedGate(t *testing.T) {
	spy := newSealedGateSpyV1()
	transport, control, err := newSealedTransportGateV1(&sealedGatePreVoteSpyV1{&sealedGateClosableSpyV1{spy}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	if err := control.releaseV1(); err != nil {
		t.Fatal(err)
	}
	transport.SetHeartbeatHandler(func(RPC) { t.Error("released heartbeat fast path invoked") })
	transport.SetHeartbeatHandler(nil)
	if spy.count("heartbeat") != 0 {
		t.Fatal("heartbeat registration reached source")
	}
	requireSealedGateRefusalV1(t, transport, spy)
	if err := transport.(WithPreVote).RequestPreVote("peer", "target", &RequestPreVoteRequest{}, &RequestPreVoteResponse{}); !errors.Is(err, errSealedTransportGateV1) || spy.count("prevote") != 0 {
		t.Fatalf("poisoned PreVote escaped: %v", err)
	}
	if err := control.releaseV1(); !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("failed gate reopened: %v", err)
	}
	if err := control.Close(); !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("gate close lost lifecycle failure: %v", err)
	}
	select {
	case _, ok := <-transport.Consumer():
		t.Fatalf("failed gate delivered or closed its consumer: open=%v", ok)
	default:
	}
}

func TestSealedTransportGateV1CloseJoinsBlockedDelivery(t *testing.T) {
	spy := newSealedGateSpyV1()
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	if err := control.releaseV1(); err != nil {
		t.Fatal(err)
	}
	select {
	case spy.consumer <- RPC{Command: "blocked-forward"}:
	case <-time.After(time.Second):
		t.Fatal("pump did not accept source RPC")
	}
	done := make(chan error, 1)
	go func() { done <- control.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not stop and join blocked pump")
	}
	select {
	case _, ok := <-transport.Consumer():
		t.Fatalf("post-close delivery or channel close: open=%v", ok)
	default:
	}
	if spy.count("close") != 1 {
		t.Fatalf("source close count=%d", spy.count("close"))
	}
	if err := control.releaseV1(); !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("release after close: %v", err)
	}
}

func TestSealedTransportGateV1ConcurrentCloseRetainsSourceError(t *testing.T) {
	spy := newSealedGateSpyV1()
	spy.closeErr = errors.New("sticky source close error")
	spy.closeEntered = make(chan struct{}, 8)
	spy.closeProceed = make(chan struct{})
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- control.Close() }()
	}
	select {
	case <-spy.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("source Close not called")
	}
	select {
	case err := <-results:
		t.Fatalf("Close returned before source result: %v", err)
	default:
	}
	close(spy.closeProceed)
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, spy.closeErr) {
				t.Fatalf("lost close error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Close stuck")
		}
	}
	if spy.count("close") != 1 {
		t.Fatalf("source closed %d times", spy.count("close"))
	}
	if err := control.Close(); !errors.Is(err, spy.closeErr) {
		t.Fatalf("close error not sticky: %v", err)
	}
	requireSealedGateRefusalV1(t, transport, spy)
}

func TestSealedTransportGateV1ReleaseCloseRace(t *testing.T) {
	for i := 0; i < 32; i++ {
		spy := newSealedGateSpyV1()
		transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		released, closed := make(chan error, 1), make(chan error, 1)
		go func() { <-start; released <- control.releaseV1() }()
		go func() { <-start; closed <- control.Close() }()
		close(start)
		select {
		case err := <-released:
			if err != nil && !errors.Is(err, errSealedTransportGateV1) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("release stuck")
		}
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("close stuck")
		}
		if spy.count("close") != 1 {
			t.Fatalf("iteration %d: source close=%d", i, spy.count("close"))
		}
		requireSealedGateRefusalV1(t, transport, spy)
		if err := control.releaseV1(); !errors.Is(err, errSealedTransportGateV1) {
			t.Fatalf("iteration %d reopened closed gate: %v", i, err)
		}
	}
}

type sealedGateBlockedOutboundSpyV1 struct {
	*sealedGateClosableSpyV1
	entered chan struct{}
	stopped chan struct{}
	finish  chan struct{}
}

func (s *sealedGateBlockedOutboundSpyV1) AppendEntries(ServerID, ServerAddress, *AppendEntriesRequest, *AppendEntriesResponse) error {
	s.record("append")
	close(s.entered)
	<-s.stopped
	<-s.finish
	return s.err
}

func (s *sealedGateBlockedOutboundSpyV1) Close() error {
	err := s.sealedGateClosableSpyV1.Close()
	close(s.stopped)
	return err
}

func TestSealedTransportGateV1CloseUnblocksThenJoinsAdmittedOutbound(t *testing.T) {
	spy := &sealedGateBlockedOutboundSpyV1{
		sealedGateClosableSpyV1: &sealedGateClosableSpyV1{newSealedGateSpyV1()},
		entered:                 make(chan struct{}), stopped: make(chan struct{}), finish: make(chan struct{}),
	}
	transport, control, err := newSealedTransportGateV1(spy)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.releaseV1(); err != nil {
		t.Fatal(err)
	}
	appended := make(chan error, 1)
	go func() {
		appended <- transport.AppendEntries("peer", "target", &AppendEntriesRequest{}, &AppendEntriesResponse{})
	}()
	select {
	case <-spy.entered:
	case <-time.After(time.Second):
		t.Fatal("outbound did not enter source")
	}
	closed := make(chan error, 1)
	go func() { closed <- control.Close() }()
	select {
	case <-spy.stopped:
	case <-time.After(time.Second):
		t.Fatal("source Close must unblock admitted outbound before joining")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before admitted outbound finished: %v", err)
	default:
	}
	close(spy.finish)
	select {
	case err := <-appended:
		if err != spy.err {
			t.Fatalf("source error lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted outbound stuck")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join admitted outbound")
	}
	if spy.count("append") != 1 || spy.count("close") != 1 {
		t.Fatalf("outbound=%d close=%d", spy.count("append"), spy.count("close"))
	}
}

func TestSealedTransportGateV1HeldPredicateAndExactOwner(t *testing.T) {
	for _, transition := range []string{"release", "close", "heartbeat"} {
		t.Run(transition, func(t *testing.T) {
			spy := &sealedGateClosableSpyV1{newSealedGateSpyV1()}
			transport, control, err := newSealedTransportGateV1(spy)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = control.Close() })
			if !control.heldV1() {
				t.Fatal("fresh gate not held")
			}
			if !control.ownsV1(transport) || control.ownsV1(spy) || control.ownsV1(struct{ Transport }{transport}) || control.ownsV1(nil) {
				t.Fatal("owner predicate accepts something other than exact returned transport")
			}
			switch transition {
			case "release":
				if err := control.releaseV1(); err != nil {
					t.Fatal(err)
				}
			case "close":
				if err := control.Close(); err != nil {
					t.Fatal(err)
				}
			case "heartbeat":
				transport.SetHeartbeatHandler(func(RPC) {})
			}
			if control.heldV1() {
				t.Fatalf("gate remained held after %s", transition)
			}
		})
	}
	spy := &sealedGatePreVoteSpyV1{&sealedGateClosableSpyV1{newSealedGateSpyV1()}}
	transport, control, err := newSealedTransportGateV1(spy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	if !control.heldV1() || !control.ownsV1(transport) || control.ownsV1(spy) || control.ownsV1(struct{ Transport }{transport}) {
		t.Fatal("optional PreVote wrapper lost exact-owner identity")
	}
}

func TestSealedTransportGateV1CloseJoinsEveryOwnedFailure(t *testing.T) {
	spy := newSealedGateSpyV1()
	spy.closeErr = errors.New("physical close failure")
	pipeline := &sealedGatePipelineSpyV1{consumer: make(chan AppendFuture), closeErr: errors.New("pipeline close failure")}
	spy.pipeline = pipeline
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	if releaseErr := control.releaseV1(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	wrapped, err := transport.AppendEntriesPipeline("peer", "target")
	if err != nil {
		t.Fatal(err)
	}
	transport.SetHeartbeatHandler(func(RPC) {})
	closed := control.Close()
	for _, want := range []error{errSealedTransportGateV1, spy.closeErr, pipeline.closeErr} {
		if !errors.Is(closed, want) {
			t.Fatalf("Close lost owned failure %v: %v", want, closed)
		}
	}
	if again := control.Close(); again != closed {
		t.Fatalf("joined Close result was not sticky: first=%v again=%v", closed, again)
	}
	if err := wrapped.Close(); err != pipeline.closeErr {
		t.Fatalf("pipeline sticky error lost: %v", err)
	}
	pipeline.mu.Lock()
	closes := pipeline.closes
	pipeline.mu.Unlock()
	if closes != 1 || spy.count("close") != 1 {
		t.Fatalf("cleanup repeated: pipeline=%d physical=%d", closes, spy.count("close"))
	}
}

func TestSealedTransportGateV1RetainsPartialPipelineCleanupFailure(t *testing.T) {
	spy := newSealedGateSpyV1()
	pipeline := &sealedGatePipelineSpyV1{consumer: make(chan AppendFuture), closeErr: errors.New("partial pipeline close failure")}
	spy.pipeline, spy.pipelineErr = pipeline, errors.New("pipeline creation failure")
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	if err := control.releaseV1(); err != nil {
		t.Fatal(err)
	}
	if result, err := transport.AppendEntriesPipeline("peer", "target"); result != nil || err != spy.pipelineErr {
		t.Fatalf("partial pipeline escaped or creation error lost: %T %v", result, err)
	}
	pipeline.mu.Lock()
	closes := pipeline.closes
	pipeline.mu.Unlock()
	if closes != 1 {
		t.Fatalf("partial pipeline cleanup count=%d", closes)
	}
	first := control.Close()
	if !errors.Is(first, pipeline.closeErr) {
		t.Fatalf("partial pipeline cleanup failure lost: %v", first)
	}
	if again := control.Close(); again != first {
		t.Fatalf("partial cleanup result not sticky: %v / %v", first, again)
	}
	pipeline.mu.Lock()
	closes = pipeline.closes
	pipeline.mu.Unlock()
	if closes != 1 || spy.count("close") != 1 {
		t.Fatalf("partial cleanup custody repeated: pipeline=%d source=%d", closes, spy.count("close"))
	}
}

var sealedGateTypedNilPipelineCallsV1 atomic.Int64

type sealedGateTypedNilPipelineV1 struct{}

func (*sealedGateTypedNilPipelineV1) AppendEntries(*AppendEntriesRequest, *AppendEntriesResponse) (AppendFuture, error) {
	sealedGateTypedNilPipelineCallsV1.Add(1)
	panic("method invoked on typed-nil pipeline")
}

func (*sealedGateTypedNilPipelineV1) Consumer() <-chan AppendFuture {
	sealedGateTypedNilPipelineCallsV1.Add(1)
	panic("method invoked on typed-nil pipeline")
}

func (*sealedGateTypedNilPipelineV1) Close() error {
	sealedGateTypedNilPipelineCallsV1.Add(1)
	panic("method invoked on typed-nil pipeline")
}

func TestSealedTransportGateV1RefusesTypedNilPipelineWithoutCallingIt(t *testing.T) {
	for _, withCreationError := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil-error", true: "creation-error"}[withCreationError], func(t *testing.T) {
			sealedGateTypedNilPipelineCallsV1.Store(0)
			spy := newSealedGateSpyV1()
			var absent *sealedGateTypedNilPipelineV1
			spy.pipeline = absent // Non-nil interface, nil concrete pipeline.
			if withCreationError {
				spy.pipelineErr = errors.New("original creation failure")
			}
			spy.closeErr = errors.New("retained physical close failure")
			transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = control.Close() })
			if err := control.releaseV1(); err != nil {
				t.Fatal(err)
			}
			var result AppendPipeline
			var creationErr error
			func() {
				defer func() {
					if caught := recover(); caught != nil {
						t.Errorf("typed-nil pipeline panicked: %v", caught)
					}
				}()
				result, creationErr = transport.AppendEntriesPipeline("peer", "target")
			}()
			if result != nil {
				t.Fatalf("typed-nil pipeline escaped as a handle: %T", result)
			}
			if withCreationError {
				if creationErr != spy.pipelineErr {
					t.Errorf("original creation error lost: %v", creationErr)
				}
			} else if !errors.Is(creationErr, errSealedTransportGateV1) {
				t.Errorf("nil-error typed-nil pipeline not refused: %v", creationErr)
			}
			if calls := sealedGateTypedNilPipelineCallsV1.Load(); calls != 0 {
				t.Errorf("typed-nil pipeline methods invoked %d times", calls)
			}
			closed := control.Close()
			if !errors.Is(closed, spy.closeErr) {
				t.Fatalf("typed-nil failure lost physical cleanup result: %v", closed)
			}
			if again := control.Close(); again != closed || spy.count("close") != 1 {
				t.Fatalf("physical cleanup not sticky/once: first=%v again=%v calls=%d", closed, again, spy.count("close"))
			}
		})
	}
}

func requireSealedGatePipelineOwnerV1(t *testing.T, gate *sealedTransportGateV1, source AppendPipeline) *sealedAppendPipelineV1 {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if len(gate.pipelines) != 1 {
		t.Fatalf("failed pipeline custody count=%d", len(gate.pipelines))
	}
	for retained := range gate.pipelines {
		if retained.source != source {
			t.Fatal("failed pipeline source was replaced")
		}
		return retained
	}
	t.Fatal("failed pipeline owner missing")
	return nil
}

func TestSealedTransportGateV1RetainsFailedPipelineObjectAndPoisonsAdmission(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "regular", true: "partial"}[partial], func(t *testing.T) {
			spy := newSealedGateSpyV1()
			source := &sealedGatePipelineSpyV1{consumer: make(chan AppendFuture), closeErr: errors.New("retained pipeline close failure")}
			spy.pipeline = source
			spy.closeErr = errors.New("retained transport close failure")
			if partial {
				spy.pipelineErr = errors.New("original pipeline creation failure")
			}
			transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = control.Close() })
			if err := control.releaseV1(); err != nil {
				t.Fatal(err)
			}
			pipeline, creationErr := transport.AppendEntriesPipeline("peer", "target")
			if partial {
				if pipeline != nil || !errors.Is(creationErr, spy.pipelineErr) {
					t.Fatalf("partial pipeline creation result=%T err=%v", pipeline, creationErr)
				}
			} else {
				if creationErr != nil || pipeline == nil {
					t.Fatalf("regular pipeline creation: %T %v", pipeline, creationErr)
				}
				if err := pipeline.Close(); !errors.Is(err, source.closeErr) {
					t.Fatalf("regular pipeline close: %v", err)
				}
			}
			retained := requireSealedGatePipelineOwnerV1(t, control, source)
			if !partial && pipeline != retained {
				t.Fatal("failed regular pipeline wrapper was replaced")
			}
			select {
			case <-control.stop:
			default:
				t.Fatal("pipeline failure did not stop delivery pump")
			}
			joined := make(chan struct{})
			go func() { control.pump.Wait(); close(joined) }()
			select {
			case <-joined:
			case <-time.After(time.Second):
				t.Fatal("failed pipeline left delivery pump running")
			}
			select {
			case spy.consumer <- RPC{Command: "forbidden-after-pipeline-failure"}:
				t.Fatal("failed gate drained new source traffic")
			default:
			}
			select {
			case _, ok := <-transport.Consumer():
				t.Fatalf("failed gate delivered or closed its private consumer: open=%v", ok)
			default:
			}
			for name, callErr := range sealedGateOutboundCallsV1(transport) {
				if !errors.Is(callErr, errSealedTransportGateV1) {
					t.Fatalf("poisoned %s not refused: %v", name, callErr)
				}
				wantCalls := 0
				if name == "pipeline" {
					wantCalls = 1
				}
				if spy.count(name) != wantCalls {
					t.Fatalf("poisoned %s reached source: calls=%d", name, spy.count(name))
				}
			}
			closed := control.Close()
			for _, want := range []error{errSealedTransportGateV1, source.closeErr, spy.closeErr} {
				if !errors.Is(closed, want) {
					t.Fatalf("gate close lost failure %v: %v", want, closed)
				}
			}
			if got := requireSealedGatePipelineOwnerV1(t, control, source); got != retained {
				t.Fatal("gate Close replaced or forgot failed pipeline wrapper")
			}
			if err := retained.Close(); !errors.Is(err, source.closeErr) {
				t.Fatalf("retained wrapper lost close failure: %v", err)
			}
			source.mu.Lock()
			closes := source.closes
			source.mu.Unlock()
			if closes != 1 || spy.count("close") != 1 {
				t.Fatalf("failed cleanup repeated: pipeline=%d transport=%d", closes, spy.count("close"))
			}
		})
	}
}

func TestSealedTransportGateV1SuccessfulPipelineCloseDoesNotCloseGate(t *testing.T) {
	spy := newSealedGateSpyV1()
	source := &sealedGatePipelineSpyV1{consumer: make(chan AppendFuture)}
	spy.pipeline = source
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	if releaseErr := control.releaseV1(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	pipeline, err := transport.AppendEntriesPipeline("peer", "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
	control.mu.Lock()
	released, closed, failure, owned := control.released, control.closed, control.failure, len(control.pipelines)
	control.mu.Unlock()
	if !released || closed || failure != nil || owned != 0 {
		t.Fatalf("successful pipeline cleanup poisoned gate: released=%v closed=%v failure=%v owned=%d", released, closed, failure, owned)
	}
	if future, err := pipeline.AppendEntries(&AppendEntriesRequest{}, &AppendEntriesResponse{}); future != nil || !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("closed pipeline reused while gate live: %T %v", future, err)
	}
	if err := transport.AppendEntries("peer", "target", &AppendEntriesRequest{}, &AppendEntriesResponse{}); err != spy.err {
		t.Fatalf("live gate no longer admits ordinary RPC: %v", err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	appends, closes := source.appends, source.closes
	source.mu.Unlock()
	if appends != 0 || closes != 1 || spy.count("append") != 1 {
		t.Fatalf("closed pipeline admitted work or repeated cleanup: appends=%d closes=%d transport appends=%d", appends, closes, spy.count("append"))
	}
}

type sealedGateBlockedPipelineV1 struct {
	*sealedGatePipelineSpyV1
	entered, stopped, finish chan struct{}
}

func (pipeline *sealedGateBlockedPipelineV1) AppendEntries(*AppendEntriesRequest, *AppendEntriesResponse) (AppendFuture, error) {
	pipeline.mu.Lock()
	pipeline.appends++
	pipeline.mu.Unlock()
	close(pipeline.entered)
	<-pipeline.stopped
	<-pipeline.finish
	return nil, pipeline.appendErr
}

func (pipeline *sealedGateBlockedPipelineV1) Close() error {
	err := pipeline.sealedGatePipelineSpyV1.Close()
	close(pipeline.stopped)
	return err
}

func TestSealedTransportGateV1PipelineCloseUnblocksThenJoinsInflightCall(t *testing.T) {
	spy := newSealedGateSpyV1()
	source := &sealedGateBlockedPipelineV1{
		sealedGatePipelineSpyV1: &sealedGatePipelineSpyV1{consumer: make(chan AppendFuture), appendErr: errors.New("admitted pipeline append result")},
		entered:                 make(chan struct{}), stopped: make(chan struct{}), finish: make(chan struct{}),
	}
	spy.pipeline = source
	transport, control, err := newSealedTransportGateV1(&sealedGateClosableSpyV1{spy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	var finishOnce sync.Once
	t.Cleanup(func() { finishOnce.Do(func() { close(source.finish) }) })
	if releaseErr := control.releaseV1(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	pipeline, err := transport.AppendEntriesPipeline("peer", "target")
	if err != nil {
		t.Fatal(err)
	}
	appended, closed := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := pipeline.AppendEntries(&AppendEntriesRequest{}, &AppendEntriesResponse{})
		appended <- err
	}()
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("pipeline append not admitted")
	}
	go func() { closed <- pipeline.Close() }()
	select {
	case <-source.stopped:
	case <-time.After(time.Second):
		t.Fatal("pipeline Close did not unblock its source before joining")
	}
	select {
	case err := <-closed:
		t.Fatalf("pipeline Close returned before admitted call joined: %v", err)
	default:
	}
	if future, err := pipeline.AppendEntries(&AppendEntriesRequest{}, &AppendEntriesResponse{}); future != nil || !errors.Is(err, errSealedTransportGateV1) {
		t.Fatalf("closing pipeline admitted another append: %T %v", future, err)
	}
	finishOnce.Do(func() { close(source.finish) })
	select {
	case err := <-appended:
		if err != source.appendErr {
			t.Fatalf("admitted source error lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted append not joined")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline Close not joined")
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	appends, closes := source.appends, source.closes
	source.mu.Unlock()
	if appends != 1 || closes != 1 {
		t.Fatalf("pipeline ownership append=%d close=%d", appends, closes)
	}
}
