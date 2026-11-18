# TODO

## Required before adopting serving restart

- Run the detached host-gate checks with Go 1.26.11, including root-module
  normal and race tests plus vet, and the focused Go 1.20.14 compatibility
  check for the sealed runtime constructor. This VM has Go 1.26.9 only.
- Review a public handoff interface and its actual elasticd owner composition.
  The current private worker/delivery prototype is not authenticated physical
  publication, a dependency adoption, or a serving-readiness result.
- Preserve the original anchor and suffix until the actual snapshot owner
  proves a durable successor; do not bypass the existing guarded Snapshot path.
- Prove restart with the real elasticd reducer before any separately authorized
  native serving run. Keep dependency tags, pins, merges and activation held.

## Later cleanup, not an initial runnable-path blocker

- Resolve the two pre-existing strict-shadow findings in upstream tests:
  `log_cache_test.go:143` and `raft_test.go:1096`. No suppression was added.
  Changed-source lint passes; a whole-fork strict-shadow run still reports
  these two findings. The original v1 lint configuration also needs a reviewed
  v2 migration before it can run unchanged with the current lint binary.
