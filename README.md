# percounter

> an amateur-ish persistent distributed [G-Counter (grow-only counter)](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#G-Counter_(Grow-only_Counter))

see tests for usage:

- [synchronous](gcounter_test.go)
- [async in-process](async_gcounter_test.go)
- [async in-process, persistent](async_gcounter_test.go)
- [async distributed (ZeroMQ via tcp), persistent](zmq_single_gcounter_test.go)

initially created to be used in [mermaidlive](https://github.com/d-led/mermaidlive)
