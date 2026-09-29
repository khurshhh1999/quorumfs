# ADR 0001: Fixed membership and strict canonical-owner quorums

Status: accepted design; Q0 implements configuration validation only.

Five fixed nodes use N=3, R=2, W=2 by default. Require 1<=R,W<=N<=5 and
R+W>N. Only canonical owners may count toward future acknowledgments; hints do
not count. This overlap does not establish linearizability. Version vectors will
preserve concurrent siblings. Reject wall-clock last-write-wins and sloppy
quorums because they would weaken the promised failure behavior.

Supported future fault model: process failure retaining disks, bounded network
partitions, and detected corruption with another verified copy. Process-kill
tests cannot establish hardware power-loss durability. Destruction of sufficient
acknowledged copies is outside the model. No dynamic membership in v1.

Q0 persists the complete fixed membership, node ID, cluster ID, epoch and quorum
configuration using synchronous RocksDB WAL writes. Changed values on the same
volume cause startup failure. Keeping the database open also prevents concurrent
use of one volume. Q0 has no object manifests, replication or data acknowledgments.
