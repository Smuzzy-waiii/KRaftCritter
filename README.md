# KRaftCritter
A Clone of KRaft, Kafka's Fault Tolerant Metadata Store which implements a subset of the interfaces defind in [KIP-746](https://cwiki.apache.org/confluence/display/KAFKA/KIP-746%3A+Revise+KRaft+Metadata+Records#KIP746:ReviseKRaftMetadataRecords-BrokerRegistrationChangeRecord). Implements RAFT Fault Tolerance using the [hashicorp/raft](https://github.com/hashicorp/raft) library.

Check out [requirements.pdf](requirements.pdf) for the exact API spec and behaviour that was followed and use [insomnia.json](https://github.com/Smuzzy-waiii/KRaftCritter/blob/main/KRaft-Insomnia.json) to test out the tool.

# Usage
```sh
$ go build .
$ mkdir /tmp/my-raft-cluster
$ mkdir /tmp/my-raft-cluster/node{A,B,C}
$ ./KRaftCritter --raft_bootstrap --raft_id=nodeA --address=localhost:50051 --raft_data_dir /tmp/my-raft-cluster
$ ./KRaftCritter --raft_id=nodeB --address=localhost:50052 --raft_data_dir /tmp/my-raft-cluster
$ ./KRaftCritter --raft_id=nodeC --address=localhost:50053 --raft_data_dir /tmp/my-raft-cluster
$ go install github.com/Jille/raftadmin/cmd/raftadmin@latest
$ raftadmin localhost:50051 add_voter nodeB localhost:50052 0
$ raftadmin --leader multi:///localhost:50051,localhost:50052 add_voter nodeC localhost:50053 0
```
**Note**: The ports provided in the CLI args are the ports used by the _grpc server_ used for internode communication. The HTTP server which serves external requests runs on _grpc_port - 10000_ on all nodes (eg: in above case the http_port will be 40051, 40052, 40053). Requests to non-leader nodes are automatically redirected to current leader node.

## References
  - https://github.com/hashicorp/raft
  - https://raft.github.io/
  - https://raft.github.io/raft.pdf
  
