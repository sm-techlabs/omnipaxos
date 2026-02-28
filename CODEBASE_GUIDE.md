# OmniPaxos Codebase Guide

This document provides a comprehensive guide to the OmniPaxos codebase, a fork of the `omnipaxos` library. It is intended to help developers understand the project's architecture, components, and overall functionality.

## 1. High-Level Architecture

The OmniPaxos project is a Rust workspace designed to provide a flexible and runtime-agnostic implementation of the Paxos consensus algorithm. The core architectural principle is the separation of the consensus logic from the application's runtime, network, and storage implementations.

This is achieved through an **event-loop-driven API**. The library's main component, the `OmniPaxos` struct, acts as a state machine. The user is responsible for driving the consensus algorithm by:
1.  Calling the `tick()` method periodically to handle time-based events like leader timeouts.
2.  Passing incoming network messages to the `handle_incoming()` method.
3.  Appending new log entries to replicate via the `append()` method.
4.  Handling the outgoing messages and log entries produced by the `OmniPaxos` instance.

This design allows OmniPaxos to be integrated into any application, regardless of the specific asynchronous runtime (e.g., Tokio, async-std) or networking stack used.

## 2. Workspace Structure

The project is organized as a Cargo workspace with several crates:

-   `omnipaxos`: The core crate containing the main consensus logic, data structures, and public API.
-   `omnipaxos_storage`: Provides different storage implementations for the consensus log and state.
-   `omnipaxos_macros`: Contains procedural macros used within the workspace.
-   `omnipaxos_ui`: A terminal-based user interface for visualizing the state of an OmniPaxos cluster.
-   `examples/`: Contains example applications demonstrating how to use the library.

## 3. Core Crate: `omnipaxos`

This crate contains the heart of the consensus algorithm.

### 3.1. Main Struct: `OmniPaxos`

The primary entry point for developers is the `OmniPaxos<T, B>` struct, found in `omnipaxos/src/omni_paxos.rs`.

-   **Generics**:
    -   `T: Entry`: The data type for log entries that the user wants to replicate.
    -   `B: Storage<T>`: The storage implementation that persists the log and other consensus state.

-   **Core Components**: Internally, an `OmniPaxos` instance is composed of:
    -   `seq_paxos`: An instance of `SequencePaxos`, which implements the core consensus logic.
    -   `ble`: An instance of `BallotLeaderElection`, responsible for the leader election protocol.
    -   `LogicalClock`s: Several clocks to manage timeouts for elections, message resends, and batch flushing.

### 3.2. Configuration

Configuration is managed by three main structs:

-   **`OmniPaxosConfig`**: The top-level configuration object. It wraps the two configs below and can be loaded from a TOML file (if the `toml_config` feature is enabled).
-   **`ClusterConfig`**: Defines cluster-wide parameters, including:
    -   `configuration_id`: A unique identifier for the current cluster configuration.
    -   `nodes`: A `Vec<NodeId>` of all servers in the cluster.
    -   `flexible_quorum`: An optional setting to specify read/write quorum sizes for different fault-tolerance trade-offs.
-   **`ServerConfig`**: Defines server-specific parameters, such as:
    -   `pid`: The unique ID of the server.
    -   `election_tick_timeout`: How many `tick()` calls before a leader election is triggered.
    -   `batch_size`: The number of log entries to batch together before sending.
    -   `leader_priority`: A value to influence which node is more likely to become a leader.

### 3.3. The Event-Loop API

The interaction with the `OmniPaxos` instance follows an event-loop pattern:

1.  **`tick()`**: The user must call this method periodically. It drives all time-dependent internal processes, including the leader election heartbeat timeout, message retransmissions, and batch flushing.
2.  **`handle_incoming(Message<T>)`**: The user passes all received network messages to this method. The `OmniPaxos` instance processes the message and updates its internal state.
3.  **`append(T)`**: The user calls this to propose a new log entry `T` to be replicated across the cluster.
4.  **`take_outgoing_messages(&mut Vec<Message<T>>)`**: After calling the methods above, the user must call this to retrieve any new messages that the instance needs to send to other nodes in the cluster. It's the user's responsibility to serialize and send these messages over their chosen network implementation.

This clear separation of concerns makes the library highly modular and independent of any specific runtime environment.

### 3.4. The Core Algorithm: `SequencePaxos`

The consensus logic is implemented in the `SequencePaxos` struct (`omnipaxos/src/sequence_paxos/`), which is internal to the `omnipaxos` crate. It acts as the engine that the `OmniPaxos` facade drives.

#### State Machine

`SequencePaxos` is a state machine driven by the `(Role, Phase)` tuple:

-   **`Role`**: Can be `Leader` or `Follower`.
-   **`Phase`**: Can be `Prepare`, `Accept`, `Recover`, or `None`.

The typical flow for a leader election is:
`Follower, None` -> `Leader, Prepare` -> `Leader, Accept`

#### Leader Logic (`leader.rs`)

1.  **Become Leader**: When a node becomes the leader (via the `BallotLeaderElection` module), it calls `handle_leader()`. It transitions to the `(Leader, Prepare)` state and sends `Prepare` messages to all followers. This starts Phase 1 of the Paxos algorithm.

2.  **Prepare Phase**: The leader collects `Promise` messages from followers. A `Promise` is a follower's pledge not to accept proposals from older leaders, and it also contains the follower's latest log information.

3.  **Majority Promises**: Once the leader receives promises from a quorum of followers (`handle_majority_promises()`), it analyzes their logs to find the most up-to-date log. It synchronizes its own log with this "super-log" and then transitions to the `(Leader, Accept)` state. This begins Phase 2.

4.  **Accept Phase**: The leader sends `AcceptSync` messages to followers to get them in sync with the new, correct log. Now, it can start accepting new user proposals (`append()`). When a new entry is proposed, the leader sends `AcceptDecide` messages to replicate it. Once a quorum of followers has acknowledged the entry, it becomes "decided" (committed).

#### Follower Logic (`follower.rs`)

1.  **Prepare**: When a follower receives a `Prepare` message from a new leader with a higher ballot number, it responds with a `Promise`. It promises not to accept older proposals and sends its own log state.

2.  **AcceptSync**: After sending a promise, the follower receives an `AcceptSync` message. This message contains the log entries it needs to become consistent with the new leader. After applying the changes, the follower enters the `(Follower, Accept)` state.

3.  **Accept**: The follower can now accept `AcceptDecide` messages, which contain new log entries to be appended. It replies with `Accepted` messages. It also handles `Decide` messages, which inform it that an entry is now committed.

This two-phase process (Prepare/Accept) ensures that only one leader can be active at a time and that no committed entries are ever lost.

### 3.5. Leader Election: `BallotLeaderElection`

Leader election is handled by the `BallotLeaderElection` (BLE) module (`omnipaxos/src/ballot_leader_election.rs`). It acts as a distributed failure detector that decides *when* a new leader should be elected.

#### The `Ballot`

At the core of BLE is the `Ballot` struct. It is a globally unique, totally ordered identifier for an election attempt. Its ordering is determined by a tuple of `(ballot_number, priority, node_id)`, which ensures there are no ties.

#### Heartbeat-Based Algorithm

The election process is driven by a periodic heartbeat mechanism, managed by the `tick()` in the main `OmniPaxos` struct.

1.  **Heartbeat Rounds**: On each `tick`, `OmniPaxos` calls `hb_timeout()` on its `BallotLeaderElection` instance. This starts a new heartbeat round. Each node sends a `HeartbeatRequest` to its peers.

2.  **Replies**: Nodes reply with a `HeartbeatReply`, which contains their own current ballot, their view of the current leader, and a crucial boolean flag: `happy`.

3.  **Happiness**: A node is "happy" if it believes there is a functioning and connected leader. If a node is the leader, it's happy if it has a quorum of followers. If it's a follower, it's happy if it is connected to a leader that is itself happy.

4.  **Takeover**: If a node finds itself "unhappy" at the end of a heartbeat round, it checks if the peers it is connected to are also unhappy. If it is connected to a quorum of unhappy nodes, it decides to take over. It increases its own ballot number (`current_ballot.n += 1`) and declares itself the new leader.

5.  **Integration**: When the BLE module elects a new leader, it informs the `OmniPaxos` struct, which in turn calls `handle_leader()` on the `SequencePaxos` instance, triggering the two-phase Paxos protocol.

### 3.6. Communication: The `Message` Enum

All communication between OmniPaxos nodes happens via the top-level `Message<T>` enum defined in `omnipaxos/src/messages.rs`. This is the type that a user's network implementation will send and receive.

The enum has two variants, corresponding to the two main components:

-   `Message::SequencePaxos(PaxosMessage<T>)`: For the core consensus algorithm.
-   `Message::BLE(BLEMessage)`: For leader election.

#### `BLEMessage`

The leader election messages are simple:
-   `HeartbeatRequest`: A broadcast from a node to its peers to check their status.
-   `HeartbeatReply`: A peer's response, containing its ballot, its leader, and its "happiness" status.

#### `PaxosMessage`

These messages map directly to the steps in the `SequencePaxos` protocol:
-   **Prepare Phase**: `PrepareReq`, `Prepare`, `Promise`.
-   **Accept Phase**: `AcceptSync`, `AcceptDecide`, `Accepted`, `Decide`.
-   **Client/Reconfig Forwarding**: `ProposalForward`, `ForwardStopSign`.
-   **Misc**: `NotAccepted`, `Compaction`, `AcceptStopSign`.

The clear separation of messages reflects the modular design of OmniPaxos, where consensus and leader election are distinct but cooperating processes.

### 3.7. The Storage Abstraction

OmniPaxos is designed to be storage-agnostic. The persistence layer is defined by the `Storage<T>` trait in `omnipaxos/src/storage/mod.rs`, which users must implement.

#### The `Storage<T>` Trait

This trait defines the interface between the consensus algorithm and the physical storage medium (e.g., in-memory, a file, a database).

-   **`Entry` and `Snapshot`**: The trait is generic over `T: Entry`. The `Entry` trait itself requires an associated `Snapshot` type, which defines how to compact the log.

-   **Atomic Writes**: The most critical method on this trait is **`write_atomically(&mut self, ops: Vec<StorageOp<T>>)`**. The consensus algorithm batches several state updates (like appending entries, updating the promised ballot, and setting the decided index) into a list of `StorageOp`s. The storage implementation **must** guarantee that this list of operations is performed atomically. If any operation fails, the entire batch must be rolled back. This is essential for preventing state corruption in case of a crash.

-   **Read Operations**: The trait provides a standard set of read methods to retrieve the consensus state, such as `get_entries()`, `get_log_len()`, `get_promise()`, and `get_decided_idx()`.

By requiring the user to provide a `Storage` implementation, OmniPaxos separates the consensus logic from the details of persistence, making the library highly flexible. The `omnipaxos_storage` crate provides some default implementations.

## 4. Crate: `omnipaxos_storage`

This crate provides two concrete implementations of the `Storage` trait defined in the core `omnipaxos` crate.

### 4.1. `MemoryStorage<T>`

-   **Description**: A simple, in-memory storage implementation.
-   **Persistence**: It stores the entire log and all protocol state (like the promised ballot) in `Vec`s and `Option`s directly in the struct. **No data is written to disk**, and all state is lost when the process terminates.
-   **Use Case**: It is primarily intended for testing, examples, and applications where durability is not a concern. It serves as a clear and simple reference for how to implement the `Storage` trait.

### 4.2. `PersistentStorage<T>`

-   **Description**: A durable, crash-safe storage implementation using [RocksDB](https://rocksdb.org/).
-   **Persistence**: It uses a local RocksDB key-value store to persist all data.
    -   **Log**: Log entries are stored in a dedicated RocksDB **Column Family**. The keys are the log indices (`u64`), and the values are the serialized entry data.
    -   **State**: Other protocol states (promised ballot, decided index, etc.) are stored as key-value pairs in the default column family.
    -   **Serialization**: The `bincode` crate is used to serialize all entries and state into byte arrays before writing them to the database.
-   **Atomicity**: It correctly implements the `write_atomically` method using a RocksDB **`WriteBatch`**. All the storage operations in the batch are prepared first and then committed to the database in a single, atomic operation. This guarantees that the storage state cannot become corrupted even if the server crashes mid-write.
-   **Use Case**: This is a production-ready storage backend for applications that require their replicated state to be durable and survive restarts and crashes.

## 5. Crate: `omnipaxos_macros`

This crate provides procedural derive macros to simplify the implementation of the `Entry` trait for log entries.

-   **`#[derive(Entry)]`**: This is the simplest macro. It generates a basic `impl Entry for MyStruct` and allows you to specify the snapshot type with a helper attribute: `#[snapshot(MySnapshotType)]`. If the attribute is omitted, it uses a placeholder `NoSnapshot` type.

-   **`#[derive(UniCacheEntry)]`**: This is a more advanced macro for enabling the UniCache feature, which can optimize network traffic for large, repetitive log entries.
    -   It automatically generates the complex `impl Entry` with all associated types required for UniCache.
    -   It generates a companion `MyStructCache` struct that holds the caching data structures.
    -   It generates the `impl UniCache for MyStructCache` with the `encode` and `decode` logic.
    -   The macro is controlled by annotating fields in the struct with `#[unicache(...)]`, allowing per-field configuration of cache size, encoding type, and eviction policy (LRU or LFU).

In short, these macros remove a significant amount of boilerplate, especially for the complex `UniCache` feature.

## 6. Crate: `omnipaxos_ui`

The `omnipaxos_ui` crate provides a real-time, terminal-based user interface (TUI) for visualizing the state and progress of an OmniPaxos node within a cluster. It's built using `ratatui` for rendering and `crossterm` for terminal interaction, and it integrates with `slog` for displaying application logs.

### 6.1. `OmniPaxosUI` (lib.rs)

This is the main orchestrator for the UI.
-   **Initialization**: Created with a `UIAppConfig` (derived from `OmniPaxosConfig`), which defines the node's `pid` and `peers`.
-   **Lifecycle**: Provides `start()` and `stop()` methods to manage the terminal's raw mode and alternate screen.
-   **Updates**: The `tick()` method is called periodically by the user, providing the latest `OmniPaxosStates` (from the core library). This method updates the internal `App` state and triggers a re-render of the UI.
-   **User Input**: It listens for keyboard events, allowing the user to quit the UI (e.g., by pressing 'q' or 'Esc').

### 6.2. `App` (app.rs)

The `App` struct holds the entire state that needs to be displayed on the UI dashboard.
-   **Node Information**: Tracks the current node's `pid`, `ballot_number`, `role` (Leader/Follower), and its view of the `current_leader`.
-   **Cluster Overview**: Stores a list of all `nodes` in the cluster, along with their assigned colors for visualization.
-   **Metrics**: Displays the `decided_idx` (last committed log entry index), `throughput_data` (entries per second), and `followers_progress` (replication progress of followers relative to the leader).

### 6.3. `render` Module (render.rs)

This module is responsible for drawing all the visual components of the dashboard using `ratatui` widgets.
-   **Dynamic Rendering**: It adapts its layout and information displayed based on whether the current node is a `Leader` or a `Follower`.
-   **Components**: Renders various UI elements, including:
    -   A title bar with basic node info.
    -   A bar chart visualizing throughput.
    -   Information panels for the current node and the cluster.
    -   A dynamic table showing the status of peers (connected, ballot, leader, accepted index).
    -   A logging window (using `tui-logger`) to display system logs.
    -   Progress bars to show follower replication status (only on the leader's view).

### 6.4. `util` Module (util.rs)

Contains helper functions and constants for the UI.
-   **Defaults**: Defines constants for UI titles, colors, and chart dimensions.
-   **Conversions**: Provides `From` trait implementations to convert `Ballot` and `OmniPaxosConfig` into UI-specific data structures, streamlining setup.

In summary, `omnipaxos_ui` offers a comprehensive, real-time graphical representation of an OmniPaxos cluster's health and activity, making it an invaluable tool for debugging, monitoring, and understanding the distributed system.

## 7. Examples

The `examples/` directory contains sample applications demonstrating how to use the OmniPaxos library.

### 7.1. `kv_store` Example

This example (`examples/kv_store/`) showcases a basic replicated Key-Value store built on top of OmniPaxos.

-   **`kv.rs`**:
    -   Defines the `KeyValue` struct, which serves as the log `Entry` type for OmniPaxos. It holds a `String` key and a `u64` value.
    -   It demonstrates the use of the `#[derive(Entry)]` macro from `omnipaxos_macros` for automatic `Entry` trait implementation.
    -   It also defines a `KVSnapshot` struct and implements the `Snapshot<KeyValue>` trait for it, illustrating how to enable and implement snapshotting for log compaction. The snapshot stores the current state of the key-value store in a `HashMap`.

-   **`util.rs`**:
    -   Contains various constants for buffer sizes, tick periods, and timeouts crucial for the simulation's network and timing parameters.

-   **`server.rs`**:
    -   Implements `OmniPaxosServer`, a wrapper around the core `OmniPaxos` instance.
    -   It simulates network communication using `tokio::sync::mpsc` channels.
    -   The `run()` method contains the server's main event loop, which handles:
        -   Periodic `tick()` calls to the `OmniPaxos` instance, driving the internal state machine (leader election, message retransmissions, batch flushing).
        -   Retrieving and sending outgoing messages generated by `OmniPaxos` to other simulated nodes.
        -   Processing incoming messages by passing them to `OmniPaxos::handle_incoming()`.

-   **`main.rs`**:
    -   **Initialization**: Sets up a multi-threaded Tokio runtime and a simulated cluster of three OmniPaxos servers. It initializes each `OmniPaxos` instance with a `MemoryStorage` backend, making the example non-persistent.
    -   **Demonstration**:
        -   It first waits for a leader to be elected.
        -   It then demonstrates appending log entries (key-value pairs) by:
            -   Sending an entry to a follower, which automatically forwards it to the leader.
            -   Sending an entry directly to the elected leader.
        -   After a short delay for entries to be decided, it reads the committed log entries and builds a local `HashMap` representing the state of the replicated key-value store.
        -   It simulates a leader failure by aborting the leader's Tokio task.
        -   It waits for a new leader to be elected and then appends another entry via the new leader.
        -   Finally, it reads the updated decided log to show the successful recovery and continued operation after a leader crash.

### 7.2. `dashboard` Example

This example (`examples/dashboard/`) demonstrates the use of the `omnipaxos_ui` crate to visualize the state of an OmniPaxos cluster in real-time.

-   **Functionality**:
    -   It sets up a simulated cluster of OmniPaxos nodes.
    -   One node is "attached" to an `OmniPaxosUI` instance, which renders the TUI dashboard.
    -   The example simulates various scenarios, such as batch appending log entries and killing the current leader to observe the election of a new leader and subsequent recovery.
-   **Key Components**:
    -   **`OmniPaxosServer`**: A wrapper that manages both the `OmniPaxos` instance and its associated `OmniPaxosUI`. In its event loop, it passes the latest state to the UI via `omni_paxos_ui.tick(omni_paxos.get_ui_states())`.
    -   **Simulation**: The `main.rs` file orchestrates the node setup, handles the initial leader election, performs appends, and simulates a node crash.

This example is the best way to see OmniPaxos in action and understand how the different components (consensus, leader election, and UI) interact in a live system.
