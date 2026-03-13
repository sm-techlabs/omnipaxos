pub mod utils;

use std::{
    thread::sleep,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kompact::prelude::{promise, Ask};
use omnipaxos::{
    messages::{
        sequence_paxos::{AcceptDecide, PaxosMessage, PaxosMsg},
        Message,
    },
    util::{LogEntry, NodeId, SequenceNumber},
};
use serial_test::serial;
use utils::{TestConfig, TestSystem, Value};

fn shutdown(mut sys: TestSystem) {
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    }
}

fn wait_until<F>(timeout: Duration, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return;
        }
        sleep(Duration::from_millis(10));
    }
    panic!("Condition was not satisfied within {:?}", timeout);
}

fn read_entries(sys: &TestSystem, pid: NodeId, upto: usize) -> Vec<LogEntry<Value>> {
    sys.nodes
        .get(&pid)
        .unwrap()
        .on_definition(|x| x.paxos.read_entries(0..upto).unwrap_or_default())
}

fn read_decided_values(sys: &TestSystem, pid: NodeId) -> Vec<Value> {
    sys.nodes.get(&pid).unwrap().on_definition(|x| {
        x.paxos
            .read_decided_suffix(0)
            .unwrap_or_default()
            .into_iter()
            .map(|entry| match entry {
                LogEntry::Decided(value) => value,
                other => panic!("Unexpected log entry: {:?}", other),
            })
            .collect()
    })
}

fn wait_for_decided_values(sys: &TestSystem, pid: NodeId, expected: &[Value], timeout: Duration) {
    wait_until(timeout, || read_decided_values(sys, pid) == expected);
}

fn inject_fast_propose(sys: &TestSystem, from: NodeId, proposal: AcceptDecide<Value>) {
    for pid in 1..=sys.nodes.len() as NodeId {
        let msg = Message::SequencePaxos(PaxosMessage {
            from,
            to: pid,
            msg: PaxosMsg::FastPropose(proposal.clone()),
        });
        sys.nodes
            .get(&pid)
            .unwrap()
            .on_definition(|x| x.paxos.handle_incoming(msg.clone()));
    }
}

#[test]
#[serial]
fn fast_path_coordinator_decides_before_cluster_wide_decide() {
    let cfg = TestConfig::default();
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = if leader == 1 { 2 } else { 1 };
    let lagging = (1..=cfg.num_nodes as NodeId)
        .find(|pid| *pid != leader && *pid != coordinator)
        .expect("Need a lagging follower");
    let first_value = Value::with_id(500);
    let second_value = Value::with_id(501);

    sys.nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.set_connection(lagging, false));

    let coordinator_node = sys.nodes.get(&coordinator).unwrap();
    let (kprom, kfuture) = promise::<()>();
    coordinator_node.on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom, first_value.clone()));
        x.paxos
            .append(first_value.clone())
            .expect("Fast-path append should succeed");
    });
    kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("Coordinator did not decide the fast-path proposal in time");

    wait_until(cfg.wait_timeout, || {
        read_entries(&sys, lagging, 1) == vec![LogEntry::Undecided(first_value.clone())]
    });

    sys.nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.set_connection(lagging, true));

    let (kprom2, kfuture2) = promise::<()>();
    coordinator_node.on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom2, second_value.clone()));
        x.paxos
            .append(second_value.clone())
            .expect("Second fast-path append should succeed");
    });
    kfuture2
        .wait_timeout(cfg.wait_timeout)
        .expect("Second fast-path proposal did not decide in time");

    let expected = vec![first_value, second_value];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    shutdown(sys);
}

/// Returns the current decided_idx at `pid`.
fn get_decided_idx(sys: &TestSystem, pid: NodeId) -> usize {
    sys.nodes
        .get(&pid)
        .unwrap()
        .on_definition(|x| x.paxos.get_decided_idx())
}

/// Waits until `pid` has `decided_idx >= target` or panics on timeout.
fn wait_for_decided_idx(sys: &TestSystem, pid: NodeId, target: usize, timeout: Duration) {
    wait_until(timeout, || get_decided_idx(sys, pid) >= target);
}

// ---------------------------------------------------------------------------
// Test 3 — Partial Fast Quorum → Slow-Path Fallback
//
// One follower is fully isolated so only 2 out of 3 nodes participate in the
// fast path.  The fast super-quorum (3) is never reached.  After the resend
// timeout, the leader detects a normal majority quorum via `is_chosen` and
// falls back to deciding through the slow path.  After the isolated follower
// is reconnected it syncs (including DOM hash via AcceptSync) and also
// eventually decides the value.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn fast_path_partial_quorum_slow_path_fallback() {
    let cfg = TestConfig::default();
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = if leader == 1 { 2 } else { 1 };
    let isolated = (1..=cfg.num_nodes as NodeId)
        .find(|pid| *pid != leader && *pid != coordinator)
        .expect("Need a third node to isolate");

    // Completely cut off `isolated` from the rest of the cluster.
    sys.set_node_connections(isolated, false);

    // Coordinator (non-leader) proposes a value via the fast path.
    let val1 = Value::with_id(600);
    let (kprom1, kfuture1) = promise::<()>();
    sys.nodes
        .get(&coordinator)
        .unwrap()
        .on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom1, val1.clone()));
            x.paxos.append(val1.clone()).expect("append should succeed");
        });

    // Only 2/3 nodes participate → fast super-quorum (3) not reached.
    // The resend-timeout fires every `resend_message_timeout` ms and the
    // leader falls back to slow-path decide via `is_chosen`.
    kfuture1
        .wait_timeout(cfg.wait_timeout)
        .expect("[Test3] coordinator did not decide val1 via slow-path fallback");

    // Leader and coordinator should both have decided.
    wait_for_decided_idx(&sys, leader, 1, cfg.wait_timeout);
    wait_for_decided_idx(&sys, coordinator, 1, cfg.wait_timeout);

    // Reconnect the isolated follower — it will sync via PrepareReq / AcceptSync.
    // Note: normal Decide messages are not proactively resent for old indexes,
    // so we do not require immediate decided_idx catch-up here.
    sys.set_node_connections(isolated, true);

    // Propose a second value with the full cluster connected to confirm the
    // system returns to normal fast-path operation.
    let val2 = Value::with_id(601);
    let (kprom2, kfuture2) = promise::<()>();
    sys.nodes
        .get(&coordinator)
        .unwrap()
        .on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom2, val2.clone()));
            x.paxos.append(val2.clone()).expect("append should succeed");
        });
    kfuture2
        .wait_timeout(cfg.wait_timeout)
        .expect("[Test3] coordinator did not decide val2");

    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_idx(&sys, pid, 2, cfg.wait_timeout);
    }

    shutdown(sys);
}

// ---------------------------------------------------------------------------
// Test 4 — Coordinator Crash
//
// The coordinator broadcasts a FastPropose and is immediately killed.
// The remaining two nodes still have the entry in their early_buffers.
// After their ticks fire they send FastAccepted to the leader.  Two out of
// three FastAccepteds satisfy the normal majority quorum; after the resend
// timeout the leader decides via slow-path fallback and broadcasts Decide to
// the surviving follower.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn fast_path_coordinator_crash_cluster_still_decides() {
    let mut cfg = TestConfig::default();
    // Use a generous timeout so the resend-timeout (500 ms) fires well within it.
    cfg.wait_timeout = Duration::from_secs(8);
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = if leader == 1 { 2 } else { 1 };
    let survivor = (1..=cfg.num_nodes as NodeId)
        .find(|pid| *pid != leader && *pid != coordinator)
        .expect("Need a surviving follower");

    let val = Value::with_id(700);

    // Coordinator sends FastPropose to everyone, then crashes immediately.
    // We inject directly so dissemination is guaranteed before crash.
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;
    let prop = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![val],
        deadline: now + 50_000,
        id: (coordinator, 700),
    };
    inject_fast_propose(&sys, coordinator, prop);
    sys.kill_node(coordinator);

    // After two FastAccepteds (leader self + survivor) the leader falls back to
    // slow-path decide at the resend timeout.
    wait_for_decided_idx(&sys, leader, 1, cfg.wait_timeout);
    wait_for_decided_idx(&sys, survivor, 1, cfg.wait_timeout);

    shutdown(sys);
}

// ---------------------------------------------------------------------------
// Test 5 — Divergent Logs → Slow-Path Reconciliation
//
// A follower is fully isolated while multiple values are decided by the rest
// of the cluster.  After reconnection the follower receives an AcceptSync that
// includes the leader's DOM hash (fix C3).  A subsequent fast-path proposal
// is processed correctly by all nodes, including the rejoined follower, and
// all nodes converge on the same final decided state.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn fast_path_divergent_logs_reconcile_after_reconnect() {
    let cfg = TestConfig::default();
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = if leader == 1 { 2 } else { 1 };
    let diverged = (1..=cfg.num_nodes as NodeId)
        .find(|pid| *pid != leader && *pid != coordinator)
        .expect("Need a node to diverge");

    // Isolate the diverging node before any proposals.
    sys.set_node_connections(diverged, false);

    // Decide two values with only the connected sub-cluster (leader + coordinator).
    let val1 = Value::with_id(800);
    let val2 = Value::with_id(801);

    let coordinator_node = sys.nodes.get(&coordinator).unwrap();
    let (kp1, kf1) = promise::<()>();
    let (kp2, kf2) = promise::<()>();
    coordinator_node.on_definition(|x| {
        x.insert_decided_future(Ask::new(kp1, val1.clone()));
        x.paxos.append(val1.clone()).expect("append val1");
    });
    kf1.wait_timeout(cfg.wait_timeout)
        .expect("[Test5] val1 not decided");

    coordinator_node.on_definition(|x| {
        x.insert_decided_future(Ask::new(kp2, val2.clone()));
        x.paxos.append(val2.clone()).expect("append val2");
    });
    kf2.wait_timeout(cfg.wait_timeout)
        .expect("[Test5] val2 not decided");

    // Both connected nodes must have decided the two values.
    wait_for_decided_idx(&sys, leader, 2, cfg.wait_timeout);
    wait_for_decided_idx(&sys, coordinator, 2, cfg.wait_timeout);

    // Reconnect the diverged node.  It will send PrepareReq → AcceptSync.
    // We do not require immediate decided_idx catch-up because old Decide
    // messages are not proactively retransmitted.
    sys.set_node_connections(diverged, true);

    // Propose one more value with the full cluster — this exercises the fast
    // path post-reconciliation and verifies all nodes converge.
    let val3 = Value::with_id(802);
    let (kp3, kf3) = promise::<()>();
    coordinator_node.on_definition(|x| {
        x.insert_decided_future(Ask::new(kp3, val3.clone()));
        x.paxos.append(val3.clone()).expect("append val3");
    });
    kf3.wait_timeout(cfg.wait_timeout)
        .expect("[Test5] val3 not decided by coordinator");

    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_idx(&sys, pid, 3, cfg.wait_timeout);
    }

    shutdown(sys);
}

#[test]
#[serial]
fn fast_path_same_deadline_tiebreaks_by_coordinator_pid() {
    let cfg = TestConfig::default();
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;
    let shared_deadline = now + 50_000;
    let first = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(20)],
        deadline: shared_deadline,
        id: (2, 200),
    };
    let second = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(30)],
        deadline: shared_deadline,
        id: (3, 300),
    };

    inject_fast_propose(&sys, 2, first);
    inject_fast_propose(&sys, 3, second);

    let expected = vec![Value::with_id(20), Value::with_id(30)];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    shutdown(sys);
}

// ---------------------------------------------------------------------------
// Large-cluster helpers
//
// Returns a TestConfig for a 7-node cluster with a longer wait timeout.
// The default 3-node config is left unchanged so existing tests are unaffected.
// ---------------------------------------------------------------------------
fn seven_node_cfg() -> TestConfig {
    TestConfig {
        num_nodes: 7,
        num_threads: 4,
        wait_timeout: Duration::from_secs(10),
        ..TestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Test 6 — Three Concurrent Coordinators (7 nodes, fast path succeeds)
//
// A 7-node cluster has fast_quorum_size = 6.  Three different non-leader
// nodes each inject a FastPropose with strictly increasing deadlines.  With
// all 7 nodes healthy all three entries collect ≥ 6 FastAccepteds and are
// decided via the fast path in deadline order.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn fast_path_three_concurrent_coordinators_seven_nodes() {
    let cfg = seven_node_cfg();
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());

    // Pick 3 non-leader pids as coordinators (smallest pids first).
    let coords: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .take(3)
        .collect();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;

    // Three proposals with strictly increasing deadlines.
    let values = [Value::with_id(1001), Value::with_id(1002), Value::with_id(1003)];
    let deadlines = [now + 50_000, now + 100_000, now + 150_000];

    for (i, (&coord, val)) in coords.iter().zip(values.iter()).enumerate() {
        let prop = AcceptDecide {
            n: ballot,
            seq_num: SequenceNumber::default(),
            decided_idx: 0,
            entries: vec![val.clone()],
            deadline: deadlines[i],
            id: (coord, 1001 + i as u64),
        };
        inject_fast_propose(&sys, coord, prop);
    }

    // fast_quorum_size(7) = 6; all 7 nodes participate → fast path succeeds.
    // Values must be decided in deadline order.
    let expected: Vec<Value> = values.to_vec();
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    shutdown(sys);
}

// ---------------------------------------------------------------------------
// Test 7 — Sub-Quorum Partition → Slow-Path Fallback (7 nodes)
//
// Two of the seven nodes are completely isolated so their outgoing
// FastAccepted messages are dropped.  Only 5 nodes respond, which is below
// fast_quorum_size(7) = 6.  The normal Paxos accept-quorum threshold is 4,
// so after the resend timeout `is_chosen` is satisfied and the leader falls
// back to the slow path to decide both entries.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn fast_path_seven_nodes_sub_quorum_slow_path_fallback() {
    let cfg = seven_node_cfg();
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());

    let non_leaders: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .collect();
    // Use the two highest-pid non-leader nodes as the isolated pair.
    let isolated: Vec<NodeId> = non_leaders[non_leaders.len() - 2..].to_vec();
    // Use the two lowest-pid non-leader nodes as coordinators.
    let coord_a = non_leaders[0];
    let coord_b = non_leaders[1];

    // Sever all connections to/from the isolated nodes.
    for &iso in &isolated {
        sys.set_node_connections(iso, false);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;

    // inject_fast_propose delivers directly to every node's inbox, bypassing
    // the connection filter.  However, the isolated nodes' outgoing buffers
    // are blocked so their FastAccepted messages never reach the leader.
    let val_a = Value::with_id(2001);
    let val_b = Value::with_id(2002);

    inject_fast_propose(
        &sys,
        coord_a,
        AcceptDecide {
            n: ballot,
            seq_num: SequenceNumber::default(),
            decided_idx: 0,
            entries: vec![val_a.clone()],
            deadline: now + 50_000,
            id: (coord_a, 2001),
        },
    );
    inject_fast_propose(
        &sys,
        coord_b,
        AcceptDecide {
            n: ballot,
            seq_num: SequenceNumber::default(),
            decided_idx: 0,
            entries: vec![val_b.clone()],
            deadline: now + 100_000,
            id: (coord_b, 2002),
        },
    );

    // Only 5 nodes (leader + coord_a + coord_b + 2 other followers) send
    // FastAccepted → fast_quorum_size(6) not met → slow-path fallback
    // triggers at the next resend_message_tick_timeout.
    let expected = vec![val_a, val_b];
    for &pid in &[leader, coord_a, coord_b] {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    shutdown(sys);
}

// ---------------------------------------------------------------------------
// Test 8 — Two Coordinators, Four Interleaved Proposals (7 nodes)
//
// Two coordinators each inject two proposals with interleaved deadlines.
// All four entries are buffered simultaneously in every node's early_buffer
// and released in strict deadline order across four successive ticks.  With
// all seven nodes healthy the fast path succeeds for every entry and the
// final decided log contains all four values in deadline order regardless of
// which coordinator originated each proposal.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn fast_path_two_coordinators_four_interleaved_proposals_seven_nodes() {
    let cfg = seven_node_cfg();
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());

    let non_leaders: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .collect();
    let coord_a = non_leaders[0];
    let coord_b = non_leaders[1];

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;

    // Four proposals with interleaved coordinators and strictly increasing deadlines.
    // Expected decided order: 3001 (A,50ms) → 3002 (B,100ms) → 3003 (A,150ms) → 3004 (B,200ms)
    let proposals: &[(NodeId, u64, i64)] = &[
        (coord_a, 3001, now + 50_000),
        (coord_b, 3002, now + 100_000),
        (coord_a, 3003, now + 150_000),
        (coord_b, 3004, now + 200_000),
    ];

    for &(coord, val_id, deadline) in proposals {
        inject_fast_propose(
            &sys,
            coord,
            AcceptDecide {
                n: ballot,
                seq_num: SequenceNumber::default(),
                decided_idx: 0,
                entries: vec![Value::with_id(val_id)],
                deadline,
                id: (coord, val_id),
            },
        );
    }

    let expected = vec![
        Value::with_id(3001),
        Value::with_id(3002),
        Value::with_id(3003),
        Value::with_id(3004),
    ];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    shutdown(sys);
}
