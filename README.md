# rkv

A lightweight distributed key-value storage built in Rust. It uses [rocksdb](https://docs.rs/rocksdb/0.18.0/rocksdb/) for the internal key-value store implementation. Distributed consensus is done using [little_raft](https://docs.rs/little_raft/0.2.0/little_raft/). The server internally communicates using gRPC but will support a HTTP-API for client access.

## Motivation

Seeing TiKV I wanted to see if the Rust ecosystem fully supports distributed systems. Few things I've noticed thus far: The support and clarity of Raft packages is very bad compared to for example Golang's [hashicorp/raft](https://github.com/hashicorp/raft). Fortunately the gRPC support and rocksdb support are very well done.
