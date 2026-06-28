# Distributed Voting & Coordination Architecture

## Current architecture

- Single `Runner.Run()` event loop probes all tenants' monitors
- Central Postgres with `tenant_id` row-level isolation
- Monolithic state machine: one node decides DOWN based on its own consecutive failures
- No coordination — each tenant's data is just partitioned, not distributed

## Target architecture

Each tenant → an independent node that probes, votes on shared endpoints, and coordinates notification ownership. This is a **single-writer → multi-writer consensus** transformation.

## Major building blocks

### 1. Node-to-node communication layer (new package)

- gRPC or simple HTTP for inter-node messages
- Node discovery (static list in config, or gossip via memberlist)
- Health check between nodes

### 2. Consensus/voting protocol (new package)

- Each node probes the same monitors independently
- Nodes broadcast their probe results to peers
- A status is only declared `DOWN` when a quorum (e.g. N/2+1) of nodes report failure
- This replaces the current single-node `consecutive_failures >= threshold` evaluation in `monitor/transition.go`

### 3. Notification ownership coordination

- Elect a "notifier" node (simplest: lowest-ID node, or etcd/raft-based leader)
- Only the notifier sends `sendIncidentNotifications()` for any given incident
- If notifier goes down, another node takes over
- Could use Postgres `SELECT ... FOR UPDATE NOWAIT` as a lightweight distributed lock on `incidents` table, avoiding a full consensus protocol

### 4. Storage changes

- Each node needs its own database (SQLite per node, or each tenant gets a separate Postgres schema/database)
- Node-local state: probe results, monitor state
- Shared/global state needs a coordination layer: at minimum a shared table for "confirmed incidents" and "notification locks"
- The current `tenant_id` column becomes a `node_id`

### 5. Runner re-architecture (`internal/app/runner.go`)

- Split the monolithic event loop: each node gets its own `Runner` instance
- Add a "gossip ticker" to exchange probe results with peers
- Replace the single `monitor.Evaluate()` with a `voting.Evaluate(quorum_of_results)`
- `retryAlertNotifications()` stays per-node but checks if it's the designated notifier first

### 6. Configuration changes (`internal/config/config.go`)

- Add a `[cluster]` section: `nodes`, `node_id`, `consensus_port`, `quorum_size`
- Each node's YAML references its peers

## The hard parts

| Area | Complexity |
|---|---|
| **Consensus correctness** | Network partitions + split-brain: two nodes may both think they're the notifier. Need fencing or leases. |
| **State reconciliation** | A node that was down for 5 minutes rejoins — how does it catch up on missed probe results and incident decisions? |
| **Notification dedup** | Two nodes might both see a DOWN transition right after the notifier dies. Need an at-least-once/at-most-once guarantee. |
| **Timing skew** | Nodes probe on slightly different schedules. Voting needs a time window, not exact alignment. |
| **Config distribution** | Adding/removing a node requires updating every other node's config. Gossip-based membership would help. |

## Minimal first step (without full Raft)

You could get 80% of the value with:

1. Each tenant → separate upag instance with its own SQLite DB
2. A shared Postgres table `voting_results(monitor_id, node_id, status, voted_at)` — each node inserts its vote
3. A new goroutine that queries `COUNT(*) ... WHERE status='DOWN'` and declares DOWN when quorum met
4. Notification lock: `INSERT INTO notification_lock(incident_id, node_id) VALUES($1, $2) ON CONFLICT DO NOTHING` — first node wins
5. A heartbeat table so nodes detect peer failures

This avoids adding Raft, gRPC, or a message bus. The central Postgres stays but becomes a coordination layer rather than a simple datastore.

**Files that change:** `runner.go`, `transition.go`, new `internal/voting/` package, `config.go`, new storage methods in `postgres.go`
