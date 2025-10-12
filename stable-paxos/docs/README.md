
Final submission for CSE 535 - Distributed Systems - Project 1
2025-10-11

## CSE 535 - Distributed Systems - Project 1 : Stable PAXOS Banking Application
This repository implements a gRPC-based distributed banking system that uses a tweaked Multi-Paxos protocol to replicate a transaction log and maintain consistent account balances across a 5-node cluster.

### Components

`PaxosCLI.java` : Interactive console tool to load transaction sets, activate servers for each set, submit transactions in order, and inspect server logs, DB state, per-sequence status, and NewView messages via gRPC calls.


`Client.java` : Banking client that builds ClientRequest messages and sends them to the cluster, preferring the known leader and falling back to broadcast with timeouts, racing for the first successful reply and updating leader hints dynamically.


`ServerNode.java` : Core Paxos server with role transitions (candidate/leader/backup), timers, executors for state/log/network/streaming/message concerns, handling Prepare/Promise, Accept/Accepted, NewView streaming, Commit, execution of committed entries in order, and client request forwarding/replies.


### Protobuf Messages and Service definitions - gRPC

`message_service.proto` : gRPC protocol and message schema for client-server and server-server RPCs: Request/Reply, Prepare/Promise, Accept/Accepted, NewView (stream), Commit, leader liveness controls, checkpoints, and CLI helpers.


### Configuration

`clientDetails.json` : Initial account universe with IDs and starting balances used to bootstrap the banking state on servers.


`serverDetails.json` : Cluster membership and networking info for servers (IDs, host, port) consumed by clients and servers to form channels and stubs.


### Test cases

`transactionSetsTest.csv` : Test scenario file supporting grouped transactions per set, live-node masks, and leader-fail markers (LF) to stress test view changes and recovery paths.

__________________________________________________

### How to Run
**Prerequisites**

    Java Development Kit (JDK): Ensure you have JDK version 17 or later installed.

**Instructions**
- The JAR files have been included in the repository for ease of use.
- They already have all dependencies bundled, including gRPC and protobuf libraries.
- The project can be run using the `run-paxos.bat` file on Windows


### Credits & Sources
- gRPC Java Documentation: https://grpc.io/docs/languages/java/
- Protocol Buffers Documentation: https://developers.google.com/protocol-buffers/docs/javat
- Multi-Paxos Algorithm: https://lamport.azurewebsites.net/pubs/paxos-simple.pdf
- Oracle docs for Java: https://docs.oracle.com/en/java/
- Stack Overflow for community support and problem-solving.

### Use of AI
- AI tools like _ChatGPT_ & _Claude_ were used to assist in code generation, debugging, and optimization.
- All AI-generated content was reviewed and modified to ensure accuracy and relevance to the project requirements.

### External libraries used
- gRPC Java for message passing and RPC framework.
- Protocol Buffers for message serialization.
- Jackson for JSON parsing.
- SLF4J for logging.
- Maven for the build system and dependency management.
- OpenCSV for CSV parsing.