// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"io"
	"reflect"
	"sync"
)

var errSealedTransportGateV1 = errors.New("sealed transport gate refused")

// sealedTransportGateV1 fences Raft delivery and dialing around one retained
// transport. It does not publish a physical listener or prove that underlying
// accepts are closed; those facts remain with the original transport owner.
type sealedTransportGateV1 struct {
	source    Transport
	closer    WithClose
	transport Transport
	addr      ServerAddress
	in        <-chan RPC
	out       chan RPC
	stop      chan struct{}

	mu          sync.Mutex
	released    bool
	closed      bool
	failure     error
	pipelineErr error
	pipelines   map[*sealedAppendPipelineV1]struct{}

	stopOnce  sync.Once
	pump      sync.WaitGroup
	calls     sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

type sealedPreVoteTransportV1 struct {
	*sealedTransportGateV1
	preVote WithPreVote
}

type sealedAppendPipelineV1 struct {
	gate     *sealedTransportGateV1
	source   AppendPipeline
	consumer <-chan AppendFuture

	mu        sync.Mutex
	closed    bool
	calls     sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func newSealedTransportGateV1(
	source Transport,
) (Transport, *sealedTransportGateV1, error) {
	if sealedTransportNilV1(source) {
		return nil, nil, errSealedTransportGateV1
	}
	closer, ok := source.(WithClose)
	if !ok || sealedTransportNilV1(closer) {
		return nil, nil, errSealedTransportGateV1
	}
	gate := &sealedTransportGateV1{
		source:    source,
		closer:    closer,
		addr:      source.LocalAddr(),
		in:        source.Consumer(),
		out:       make(chan RPC),
		stop:      make(chan struct{}),
		pipelines: make(map[*sealedAppendPipelineV1]struct{}),
	}
	if gate.in == nil {
		return nil, nil, errSealedTransportGateV1
	}
	// A retained physical transport must begin without an old fast-path.
	source.SetHeartbeatHandler(nil)
	var transport Transport = gate
	if preVote, supported := source.(WithPreVote); supported &&
		!sealedTransportNilV1(preVote) {
		transport = &sealedPreVoteTransportV1{
			sealedTransportGateV1: gate,
			preVote:               preVote,
		}
	}
	gate.transport = transport
	return transport, gate, nil
}

func sealedTransportNilV1(value interface{}) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (gate *sealedTransportGateV1) ownsV1(transport Transport) bool {
	if gate == nil {
		return false
	}
	switch owned := gate.transport.(type) {
	case *sealedTransportGateV1:
		candidate, ok := transport.(*sealedTransportGateV1)
		return ok && candidate == owned
	case *sealedPreVoteTransportV1:
		candidate, ok := transport.(*sealedPreVoteTransportV1)
		return ok && candidate == owned
	default:
		return false
	}
}

func (gate *sealedTransportGateV1) heldV1() bool {
	if gate == nil {
		return false
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return !gate.released && !gate.closed && gate.failure == nil
}

func (gate *sealedTransportGateV1) Consumer() <-chan RPC {
	if gate == nil {
		return nil
	}
	return gate.out
}

func (gate *sealedTransportGateV1) LocalAddr() ServerAddress {
	if gate == nil {
		return ""
	}
	return gate.addr
}

func (gate *sealedTransportGateV1) releaseV1() error {
	if gate == nil {
		return errSealedTransportGateV1
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.closed || gate.released || gate.failure != nil {
		return errSealedTransportGateV1
	}
	gate.released = true
	gate.pump.Add(1)
	go gate.pumpV1()
	return nil
}

func (gate *sealedTransportGateV1) pumpV1() {
	defer gate.pump.Done()
	for {
		select {
		case <-gate.stop:
			return
		case rpc, open := <-gate.in:
			if !open {
				gate.failV1()
				return
			}
			select {
			case gate.out <- rpc:
			case <-gate.stop:
				return
			}
		}
	}
}

func (gate *sealedTransportGateV1) failV1() {
	gate.mu.Lock()
	if !gate.closed && gate.failure == nil {
		gate.failure = errSealedTransportGateV1
	}
	gate.mu.Unlock()
	gate.stopOnce.Do(func() { close(gate.stop) })
}

// failedV1 reports only a local gate poison. Ordinary remote RPC errors are
// returned to their callers and never change this state.
func (gate *sealedTransportGateV1) failedV1() bool {
	if gate == nil {
		return false
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.failure != nil
}

func (gate *sealedTransportGateV1) beginV1() bool {
	if gate == nil {
		return false
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if !gate.released || gate.closed || gate.failure != nil {
		return false
	}
	gate.calls.Add(1)
	return true
}

func (gate *sealedTransportGateV1) endV1() {
	gate.calls.Done()
}

func (gate *sealedTransportGateV1) AppendEntriesPipeline(
	id ServerID,
	target ServerAddress,
) (AppendPipeline, error) {
	if !gate.beginV1() {
		return nil, errSealedTransportGateV1
	}
	defer gate.endV1()
	source, err := gate.source.AppendEntriesPipeline(id, target)
	if err != nil {
		if !sealedTransportNilV1(source) {
			pipeline := &sealedAppendPipelineV1{
				gate: gate, source: source,
			}
			gate.adoptPipelineV1(pipeline)
			_ = pipeline.Close()
		}
		return nil, err
	}
	if sealedTransportNilV1(source) {
		return nil, errSealedTransportGateV1
	}
	pipeline := &sealedAppendPipelineV1{
		gate: gate, source: source, consumer: source.Consumer(),
	}
	admitted := gate.adoptPipelineV1(pipeline)
	if pipeline.consumer == nil {
		_ = pipeline.Close()
		return nil, errSealedTransportGateV1
	}
	if !admitted {
		_ = pipeline.Close()
		return nil, errSealedTransportGateV1
	}
	return pipeline, nil
}

func (gate *sealedTransportGateV1) adoptPipelineV1(
	pipeline *sealedAppendPipelineV1,
) bool {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.pipelines[pipeline] = struct{}{}
	return !gate.closed && gate.released && gate.failure == nil
}

func (gate *sealedTransportGateV1) AppendEntries(
	id ServerID,
	target ServerAddress,
	request *AppendEntriesRequest,
	response *AppendEntriesResponse,
) error {
	if !gate.beginV1() {
		return errSealedTransportGateV1
	}
	defer gate.endV1()
	return gate.source.AppendEntries(id, target, request, response)
}

func (gate *sealedTransportGateV1) RequestVote(
	id ServerID,
	target ServerAddress,
	request *RequestVoteRequest,
	response *RequestVoteResponse,
) error {
	if !gate.beginV1() {
		return errSealedTransportGateV1
	}
	defer gate.endV1()
	return gate.source.RequestVote(id, target, request, response)
}

func (gate *sealedTransportGateV1) InstallSnapshot(
	id ServerID,
	target ServerAddress,
	request *InstallSnapshotRequest,
	response *InstallSnapshotResponse,
	reader io.Reader,
) error {
	if !gate.beginV1() {
		return errSealedTransportGateV1
	}
	defer gate.endV1()
	return gate.source.InstallSnapshot(
		id, target, request, response, reader,
	)
}

func (gate *sealedTransportGateV1) EncodePeer(
	id ServerID,
	address ServerAddress,
) []byte {
	if !gate.beginV1() {
		return nil
	}
	defer gate.endV1()
	return gate.source.EncodePeer(id, address)
}

func (gate *sealedTransportGateV1) DecodePeer(
	encoded []byte,
) ServerAddress {
	if !gate.beginV1() {
		return ""
	}
	defer gate.endV1()
	return gate.source.DecodePeer(encoded)
}

func (gate *sealedTransportGateV1) SetHeartbeatHandler(
	callback func(RPC),
) {
	if gate == nil || callback == nil {
		return
	}
	gate.failV1()
}

func (gate *sealedTransportGateV1) TimeoutNow(
	id ServerID,
	target ServerAddress,
	request *TimeoutNowRequest,
	response *TimeoutNowResponse,
) error {
	if !gate.beginV1() {
		return errSealedTransportGateV1
	}
	defer gate.endV1()
	return gate.source.TimeoutNow(id, target, request, response)
}

func (transport *sealedPreVoteTransportV1) RequestPreVote(
	id ServerID,
	target ServerAddress,
	request *RequestPreVoteRequest,
	response *RequestPreVoteResponse,
) error {
	if transport == nil || !transport.beginV1() {
		return errSealedTransportGateV1
	}
	defer transport.endV1()
	return transport.preVote.RequestPreVote(id, target, request, response)
}

func (gate *sealedTransportGateV1) Close() error {
	if gate == nil {
		return errSealedTransportGateV1
	}
	gate.closeOnce.Do(func() {
		gate.mu.Lock()
		gate.closed = true
		gate.stopOnce.Do(func() { close(gate.stop) })
		lifecycleErr := gate.failure
		pipelines := make([]*sealedAppendPipelineV1, 0, len(gate.pipelines))
		for pipeline := range gate.pipelines {
			pipelines = append(pipelines, pipeline)
		}
		gate.mu.Unlock()

		physicalErr := gate.closer.Close()
		for _, pipeline := range pipelines {
			_ = pipeline.Close()
		}
		gate.pump.Wait()
		gate.calls.Wait()
		gate.mu.Lock()
		pipelineErr := gate.pipelineErr
		gate.mu.Unlock()
		gate.closeErr = errors.Join(lifecycleErr, physicalErr, pipelineErr)
	})
	return gate.closeErr
}

func (pipeline *sealedAppendPipelineV1) AppendEntries(
	request *AppendEntriesRequest,
	response *AppendEntriesResponse,
) (AppendFuture, error) {
	if !pipeline.beginV1() {
		return nil, errSealedTransportGateV1
	}
	defer pipeline.endV1()
	return pipeline.source.AppendEntries(request, response)
}

func (pipeline *sealedAppendPipelineV1) beginV1() bool {
	if pipeline == nil {
		return false
	}
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	if pipeline.closed || !pipeline.gate.beginV1() {
		return false
	}
	pipeline.calls.Add(1)
	return true
}

func (pipeline *sealedAppendPipelineV1) endV1() {
	pipeline.gate.endV1()
	pipeline.calls.Done()
}

func (pipeline *sealedAppendPipelineV1) Consumer() <-chan AppendFuture {
	if pipeline == nil {
		return nil
	}
	return pipeline.consumer
}

func (pipeline *sealedAppendPipelineV1) Close() error {
	if pipeline == nil {
		return errSealedTransportGateV1
	}
	pipeline.closeOnce.Do(func() {
		pipeline.mu.Lock()
		pipeline.closed = true
		pipeline.mu.Unlock()
		pipeline.closeErr = pipeline.source.Close()
		pipeline.calls.Wait()
		pipeline.gate.finishPipelineCloseV1(
			pipeline,
			pipeline.closeErr,
		)
	})
	return pipeline.closeErr
}

func (gate *sealedTransportGateV1) finishPipelineCloseV1(
	pipeline *sealedAppendPipelineV1,
	err error,
) {
	gate.mu.Lock()
	if err == nil {
		delete(gate.pipelines, pipeline)
	} else {
		gate.pipelineErr = errors.Join(gate.pipelineErr, err)
		if gate.failure == nil {
			gate.failure = errSealedTransportGateV1
		}
	}
	gate.mu.Unlock()
	if err != nil {
		gate.stopOnce.Do(func() { close(gate.stop) })
	}
}
