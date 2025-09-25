# Client

## Overview

The `Client` class represents a client node in a MultiPaxos distributed system. It is responsible for generating client requests, managing server configurations, and interacting with the leader server. The client reads server details from a configuration file and can generate protocol buffer messages to initiate transactions.

## Fields

- **logger**: Log4j logger for error and event logging.
- **filePath**: Path to the server configuration JSON file.
- **clientId**: Unique identifier for the client.
- **servers**: Map of server IDs to their connection details.
- **leaderId**: Current leader server's ID.
- **timer**: Timer for managing client-side timeouts.

## Constructors

- **Client(String clientId)**  
  Initializes the client with a default configuration file path.

- **Client(String clientId, String filePath)**  
  Initializes the client with a user-specified configuration file path.

## Methods

### `static HashMap<String, ServerDetails> loadServersFromConfig(String filePath)`

Loads server details from a JSON configuration file and returns a map of server IDs to `ServerDetails` objects.

### `String getClientId()`

Returns the unique client ID.

### `String getLeaderId()`

Returns the current leader server's ID.

### `static MessageServiceOuterClass.ClientRequest generateClientRequest(String sender, String receiver, double amount, long timestamp, String clientId)`

Generates a protocol buffer `ClientRequest` message for a transaction, including sender, receiver, amount, timestamp, and client ID.

### `static void main(String[] args)`

Entry point for the client application. Initializes a client and logs any errors during startup.

## Usage in MultiPaxos

The `Client` class is responsible for initiating requests that will be proposed and agreed upon by the MultiPaxos cluster. It interacts with the leader server, sending transaction requests that are replicated and agreed upon using the MultiPaxos protocol.

---
