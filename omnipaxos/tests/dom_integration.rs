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
            .flat_map(|entry| match entry {
                LogEntry::Decided(value) => vec![value],
                LogEntry::Snapshotted(snapshot) => snapshot.snapshot.snapshotted,
                LogEntry::Undecided(_) | LogEntry::Trimmed(_) | LogEntry::StopSign(_, _) => {
                    Vec::new()
                }
            })
            .collect()
    })
}

fn wait_for_decided_values(sys: &TestSystem, pid: NodeId, expected: &[Value], timeout: Duration) {
    wait_until(timeout, || read_decided_values(sys, pid) == expected);
}

fn coordinator_append(sys: &TestSystem, coordinator: NodeId, value: Value) {
    sys.nodes
        .get(&coordinator)
        .unwrap()
        .on_definition(|x| x.paxos.append(value).expect("Append should succeed"));
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

const SEP: &str = "══════════════════════════════════════════════════════════════════════";

fn test_begin(name: &str) {
    eprintln!("\n\x1b[36m{SEP}\x1b[0m");
    eprintln!("\x1b[36m  ▶ BEGIN  {name}\x1b[0m");
    eprintln!("\x1b[36m{SEP}\x1b[0m");
}

fn test_end(name: &str) {
    eprintln!("\x1b[36m{SEP}\x1b[0m");
    eprintln!("\x1b[36m  ✓ END    {name}\x1b[0m");
    eprintln!("\x1b[36m{SEP}\x1b[0m\n");
}

fn print_final_logs(sys: &TestSystem, test_name: &str) {
    eprintln!("\x1b[35m┌─ Final logs for {test_name}\x1b[0m");

    let mut pids: Vec<NodeId> = sys.nodes.keys().copied().collect();
    pids.sort_unstable();

    for pid in pids {
        let (decided_idx, entries) = sys.nodes.get(&pid).unwrap().on_definition(|x| {
            let decided_idx = x.paxos.get_decided_idx();
            let entries = x.paxos.read_entries(0..decided_idx).unwrap_or_default();
            (decided_idx, entries)
        });

        eprintln!(
            "\x1b[35m├─ Node {pid}  (decided_idx={decided_idx}, entries={})\x1b[0m",
            entries.len()
        );

        if entries.is_empty() {
            eprintln!("│   (empty)");
            continue;
        }

        for (idx, entry) in entries.iter().enumerate() {
            match entry {
                LogEntry::Decided(value) => eprintln!("│   [{idx:>2}] DECIDED    {value:?}"),
                LogEntry::Undecided(value) => eprintln!("│   [{idx:>2}] UNDECIDED  {value:?}"),
                LogEntry::Snapshotted(snapshot) => eprintln!(
                    "│   [{idx:>2}] SNAPSHOT   trimmed_idx={} snapshot={:?}",
                    snapshot.trimmed_idx, snapshot.snapshot
                ),
                LogEntry::StopSign(stopsign, is_decided) => eprintln!(
                    "│   [{idx:>2}] STOPSIGN   decided={} {stopsign:?}",
                    is_decided
                ),
                LogEntry::Trimmed(trimmed_idx) => {
                    eprintln!("│   [{idx:>2}] TRIMMED    up_to={trimmed_idx}")
                }
            }
        }
    }

    eprintln!("\x1b[35m└─ End final logs\x1b[0m");
}

fn dom_default_testcfg(num_nodes: Option<usize>) -> TestConfig {
    let num_nodes = num_nodes.unwrap_or(7);
    TestConfig {
        num_nodes,
        num_threads: num_nodes,
        wait_timeout: Duration::from_millis(10_000),
        ..TestConfig::default()
    }
}

#[test]
#[serial]
fn fast_path_coordinator_decides_before_cluster_wide_decide() {
    test_begin("fast_path_coordinator_decides_before_cluster_wide_decide");
    let cfg = dom_default_testcfg(None);
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

    print_final_logs(&sys, "fast_path_coordinator_decides_before_cluster_wide_decide");
    test_end("fast_path_coordinator_decides_before_cluster_wide_decide");
    shutdown(sys);
}

/// Test 3 — Partial Fast Quorum falls back to Slow Path
///
/// With N=3 and fast_quorum=3, blocking the coordinator's outgoing link to
/// one follower means only 2 FastAccepted messages reach the leader (below the
/// super quorum).  The system must detect the stall on the resend tick and
/// fall back to the slow path (regular quorum decide + AcceptDecide to the
/// lagging follower).  After reconnection, all three nodes must eventually
/// decide both values.
#[test]
#[serial]
fn partial_fast_quorum_falls_back_to_slow_path() {
    test_begin("partial_fast_quorum_falls_back_to_slow_path");
    let cfg = dom_default_testcfg(None);
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = if leader == 1 { 2 } else { 1 };
    let lagging = (1..=cfg.num_nodes as NodeId)
        .find(|pid| *pid != leader && *pid != coordinator)
        .expect("Need a lagging follower");
    let first_value = Value::with_id(600);
    let second_value = Value::with_id(601);

    // Block the coordinator's outgoing link to lagging so lagging never
    // receives the FastPropose for first_value.  This ensures only 2 out of 3
    // FastAccepted messages reach the leader, preventing a fast-quorum decide.
    sys.nodes
        .get(&coordinator)
        .unwrap()
        .on_definition(|x| x.set_connection(lagging, false));

    let coordinator_node = sys.nodes.get(&coordinator).unwrap();
    coordinator_node.on_definition(|x| {
        x.paxos
            .append(first_value.clone())
            .expect("Fast-path append should succeed");
    });

    // Wait for the slow-path fallback: the resend timer fires, detects the
    // stall (accepted_idx > decided_idx, regular quorum met), decides via slow
    // path, and also sends a fallback AcceptDecide to lagging.
    wait_for_decided_values(&sys, coordinator, &[first_value.clone()], cfg.wait_timeout);
    wait_for_decided_values(&sys, leader, &[first_value.clone()], cfg.wait_timeout);

    // Restore the coordinator→lagging link and append a second value.
    // All three nodes now participate in the fast path for second_value.
    sys.nodes
        .get(&coordinator)
        .unwrap()
        .on_definition(|x| x.set_connection(lagging, true));

    let (kprom, kfuture) = promise::<()>();
    coordinator_node.on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom, second_value.clone()));
        x.paxos
            .append(second_value.clone())
            .expect("Second append should succeed");
    });
    kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("Second value did not decide in time");

    let expected = vec![first_value, second_value];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    print_final_logs(&sys, "partial_fast_quorum_falls_back_to_slow_path");
    test_end("partial_fast_quorum_falls_back_to_slow_path");
    shutdown(sys);
}

/// Test 4 — Coordinator Crash: system still eventually decides
///
/// The coordinator broadcasts FastPropose then is killed before it can send its
/// own FastAccepted (or before the super quorum is reached).  With only 2 out
/// of 3 nodes left alive, fast_quorum=3 is never met.  The leader's resend
/// timer must detect the stall (regular quorum met) and decide via the slow
/// path so the two surviving nodes commit the entry.
#[test]
#[serial]
fn coordinator_crash_still_decides() {
    test_begin("coordinator_crash_still_decides");
    let cfg = dom_default_testcfg(Some(3));
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = if leader == 1 { 2 } else { 1 };
    let first_value = Value::with_id(700);

    // Append triggers fast_propose_local which immediately queues FastPropose
    // messages in the outgoing buffer.  The Kompact timer sends them within 1 ms.
    sys.nodes.get(&coordinator).unwrap().on_definition(|x| {
        x.paxos
            .append(first_value.clone())
            .expect("Append should succeed");
    });

    // Give the messages time to propagate before killing the coordinator.
    sleep(Duration::from_millis(50));
    sys.kill_node(coordinator);

    // After the DOM deadline expires the two surviving nodes release
    // first_value from their early buffers, accept it, and send FastAccepted
    // to the leader.  Only 2 FastAccepted arrive (below super quorum 3) so
    // fast_decide is not triggered.  The resend timer then detects the stall
    // (regular quorum = 2 out of remaining 2 nodes) and decides via slow path.
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_for_decided_values(&sys, pid, &[first_value.clone()], cfg.wait_timeout);
    }

    print_final_logs(&sys, "coordinator_crash_still_decides");
    test_end("coordinator_crash_still_decides");
    shutdown(sys);
}

/// Test 5 — Divergent Logs reconcile via Slow Path (AcceptSync)
///
/// A follower that was fully partitioned for multiple fast-path proposals ends
/// up with an empty log while the rest of the cluster has committed several
/// entries.  When the follower reconnects and a new proposal arrives, the
/// per-follower seq_num advanced by the leader's resend fallback causes the
/// follower to detect the gap (DroppedPreceding) and trigger Phase 1 recovery
/// (PrepareReq → Prepare → Promise → AcceptSync), bringing all nodes into
/// agreement.
#[test]
#[serial]
fn divergent_logs_reconcile_via_slow_path() {
    test_begin("divergent_logs_reconcile_via_slow_path");
    let cfg = dom_default_testcfg(None);
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = if leader == 1 { 2 } else { 1 };
    let lagging = (1..=cfg.num_nodes as NodeId)
        .find(|pid| *pid != leader && *pid != coordinator)
        .expect("Need a lagging follower");
    let v1 = Value::with_id(800);
    let v2 = Value::with_id(801);
    let v3 = Value::with_id(802);

    // Completely isolate lagging so it misses both fast-path proposals.
    sys.set_node_connections(lagging, false);

    coordinator_append(&sys, coordinator, v1.clone());
    coordinator_append(&sys, coordinator, v2.clone());

    // Wait for the two connected nodes to decide both values via slow-path
    // fallback.  The resend tick also advances the leader's seq_num counter
    // for lagging (fallback AcceptDecide is queued but can't be delivered).
    for pid in [leader, coordinator] {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 2
            })
        });
    }

    // Reconnect lagging and append a third value.  The AcceptDecide for v3
    // will carry a seq_num that is ahead of lagging's current_seq_num
    // (DroppedPreceding), causing lagging to call reconnected() and initiate
    // Phase 1 recovery.  The resulting AcceptSync delivers v1+v2+v3 to lagging.
    sys.set_node_connections(lagging, true);

    let (kprom, kfuture) = promise::<()>();
    sys.nodes.get(&coordinator).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom, v3.clone()));
        x.paxos.append(v3.clone()).expect("Append should succeed");
    });
    kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("v3 did not decide on coordinator in time");

    // Verify all nodes have decided all 3 entries.  We check decided_idx
    // rather than reading the log contents because OmniPaxos may have
    // snapshotted entries during the AcceptSync recovery, which would cause
    // read_decided_values to panic on Snapshotted log entries.
    let expected_decided_idx = 3;
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= expected_decided_idx
            })
        });
    }

    print_final_logs(&sys, "divergent_logs_reconcile_via_slow_path");
    test_end("divergent_logs_reconcile_via_slow_path");
    shutdown(sys);
}

#[test]
#[serial]
fn fast_path_same_deadline_tiebreaks_by_coordinator_pid() {
    test_begin("fast_path_same_deadline_tiebreaks_by_coordinator_pid");
    let cfg = dom_default_testcfg(None);
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

    print_final_logs(&sys, "fast_path_same_deadline_tiebreaks_by_coordinator_pid");
    test_end("fast_path_same_deadline_tiebreaks_by_coordinator_pid");
    shutdown(sys);
}

/// Test — Seven nodes, three coordinators with distinct deadlines.
///
/// With N=7 the fast quorum is 6.  All 7 nodes receive all three FastPropose
/// messages so each entry collects 7 FastAccepted responses — above the super
/// quorum threshold — and every entry is fast-path decided.
///
/// The three proposals come from coordinators with pids 2, 3, and 4, each
/// carrying a strictly increasing deadline so the deadline-ordered release
/// produces a deterministic global log order: value 10 first, then 20, then 30.
#[test]
#[serial]
fn seven_nodes_three_coordinators_deadline_ordering() {
    test_begin("seven_nodes_three_coordinators_deadline_ordering");
    let cfg = dom_default_testcfg(None);
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

    // Three coordinators, strictly increasing deadlines → deterministic order.
    let proposal_a = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(10)],
        deadline: now + 50_000,
        id: (2, 1000),
    };
    let proposal_b = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(20)],
        deadline: now + 100_000,
        id: (3, 2000),
    };
    let proposal_c = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(30)],
        deadline: now + 150_000,
        id: (4, 3000),
    };

    // Broadcast all three proposals to every node simultaneously.
    inject_fast_propose(&sys, 2, proposal_a);
    inject_fast_propose(&sys, 3, proposal_b);
    inject_fast_propose(&sys, 4, proposal_c);

    // All 7 nodes accept all 3 entries in deadline order and fast-decide.
    // Fast quorum for N=7 is 6; with all 7 participating every entry clears it.
    let expected = vec![Value::with_id(10), Value::with_id(20), Value::with_id(30)];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    print_final_logs(&sys, "seven_nodes_three_coordinators_deadline_ordering");
    test_end("seven_nodes_three_coordinators_deadline_ordering");
    shutdown(sys);
}

/// Test — Seven nodes, three concurrent coordinators, one node fully partitioned.
///
/// N=7, fast_quorum=6.  With one node fully isolated from the cluster exactly
/// 6 nodes participate in each fast-path round, which is exactly the quorum
/// threshold so all three concurrent proposals fast-decide on the six connected
/// nodes.
///
/// After reconnecting the partitioned node a fourth proposal is injected.  The
/// leader's Decide for proposal 4 carries a DOM hash that spans all four
/// entries.  The reconnected node, whose DOM log only contains entry 4, sees a
/// hash mismatch, calls reconnected(), and recovers via Phase 1
/// (PrepareReq → AcceptSync), bringing all seven nodes into agreement.
#[test]
#[serial]
fn seven_nodes_multiple_coordinators_straggler_recovers() {
    test_begin("seven_nodes_multiple_coordinators_straggler_recovers");
    let cfg = dom_default_testcfg(None);
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Node 7 is completely partitioned; it misses all three initial proposals.
    let straggler: NodeId = 7;
    sys.set_node_connections(straggler, false);

    // Three distinct coordinators propose concurrently.
    let c1 = (1..=cfg.num_nodes as NodeId)
        .find(|&pid| pid != leader && pid != straggler)
        .expect("c1");
    let c2 = (1..=cfg.num_nodes as NodeId)
        .find(|&pid| pid != leader && pid != straggler && pid != c1)
        .expect("c2");
    let c3 = (1..=cfg.num_nodes as NodeId)
        .find(|&pid| pid != leader && pid != straggler && pid != c1 && pid != c2)
        .expect("c3");

    let v1 = Value::with_id(900);
    let v2 = Value::with_id(901);
    let v3 = Value::with_id(902);
    let v4 = Value::with_id(903);

    // Issue all three proposals at the same time.
    for (coord, val) in [(c1, v1.clone()), (c2, v2.clone()), (c3, v3.clone())] {
        sys.nodes.get(&coord).unwrap().on_definition(|x| {
            x.paxos.append(val).expect("append should succeed");
        });
    }

    // The six active nodes participate → fast_quorum=6 is exactly met.
    // Wait for them (excluding the straggler) to decide all three entries.
    let active: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&pid| pid != straggler)
        .collect();
    for pid in &active {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 3
            })
        });
    }

    // Reconnect the straggler and inject a fourth proposal so that every node
    // participates in the fast path.  The Decide for entry 4 carries the
    // cumulative DOM hash of all four entries.  The straggler's DOM log only
    // has entry 4 (hash mismatch) → reconnected() → Phase 1 recovery.
    sys.set_node_connections(straggler, true);

    let (kprom, kfuture) = promise::<()>();
    sys.nodes.get(&c1).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom, v4.clone()));
        x.paxos.append(v4.clone()).expect("append should succeed");
    });
    kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("v4 did not decide in time");

    // After Phase 1 recovery the straggler must have all four entries committed.
    // We check decided_idx (not the raw log) because snapshotting may occur
    // during AcceptSync.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 4
            })
        });
    }

    print_final_logs(&sys, "seven_nodes_multiple_coordinators_straggler_recovers");
    test_end("seven_nodes_multiple_coordinators_straggler_recovers");
    shutdown(sys);
}

/// Happy path — Fast Path end-to-end
///
/// This test documents the complete flow of a single client request through the
/// DOM fast path on a 3-node cluster where all nodes are connected.
///
/// Expected event sequence (visible in logs with `--features logging`):
///   coordinator  → [APPEND]        fast_propose broadcasts FastPropose to all nodes
///   every node   → [RECV]          FastPropose received
///   every node   → (tick fires)    [FAST_PATH][BUFFER] deadline expires, entry released
///   every node   → [RECV][ACCEPT_DECIDE] entry appended to local log (fast_path=true)
///   every node   → [SEND][FAST_ACCEPTED] FastAccepted sent to leader
///   leader       → [RECV]          FastAccepted received from each node
///   leader       → (fast quorum)   [FAST_DECIDE] decided_idx advanced, Decide broadcast
///   coordinator  → [FAST_PATH][DECIDE] decided_idx advanced (1-RTT client reply)
///   every node   → [RECV][DECIDE]  Decide received, committed
#[test]
#[serial]
fn happy_path_fast_path() {
    test_begin("happy_path_fast_path");
    let cfg = dom_default_testcfg(Some(3)); // 3 nodes, all connected
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    // Pick a non-leader as coordinator so we exercise the full
    // FastReply → coordinator → leader → Decide path.
    let coordinator = (1..=cfg.num_nodes as NodeId)
        .find(|&pid| pid != leader)
        .expect("coordinator");
    let value = Value::with_id(1001);

    // Register a decided-future on the coordinator so we can block until
    // the fast-path 1-RTT reply arrives.
    let (kprom, kfuture) = promise::<()>();
    sys.nodes.get(&coordinator).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom, value.clone()));
        x.paxos.append(value.clone()).expect("append should succeed");
    });

    // The coordinator must decide via the fast path before the cluster-wide
    // Decide is broadcast by the leader.
    kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("coordinator did not receive fast-path decide in time");

    // All three replicas must eventually commit the value.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &[value.clone()], cfg.wait_timeout);
    }

    print_final_logs(&sys, "happy_path_fast_path");
    test_end("happy_path_fast_path");
    shutdown(sys);
}

/// Happy path — Slow Path end-to-end
///
/// This test documents the complete flow of a single client request through the
/// slow path (resend-timer fallback) on a 3-node cluster.
///
/// The coordinator's outgoing link to one follower is severed so the fast-path
/// super quorum (N=3 requires all 3) can never be reached.  After the resend
/// timer fires the leader detects the stall (regular quorum already met) and
/// commits via the slow path, also sending a fallback AcceptDecide to the
/// lagging follower.
///
/// Expected event sequence (visible in logs with `--features logging`):
///   coordinator  → [APPEND]        fast_propose broadcasts FastPropose (lagging misses it)
///   2 nodes      → (tick fires)    entry released from DOM buffer
///   2 nodes      → [SEND][FAST_ACCEPTED] only 2 FastAccepted reach leader (<super quorum)
///   resend timer → [SLOW_PATH][DECIDE] quorum met → set_decided_idx, Decide(hash=0) broadcast
///   leader       → [SEND][DECIDE]  Decide sent to connected peers (hash=0 = slow-path)
///   leader       → [SEND][ACCEPT_DECIDE] fallback AcceptDecide sent to lagging follower
///   lagging      → [RECV][ACCEPT_DECIDE] entry appended
///   lagging      → [RECV][DECIDE]  committed (hash=0 → no DOM hash check)
///   all nodes    → decided_idx == 1
#[test]
#[serial]
fn happy_path_slow_path() {
    test_begin("happy_path_slow_path");
    let cfg = dom_default_testcfg(Some(3)); // 3 nodes
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = (1..=cfg.num_nodes as NodeId)
        .find(|&pid| pid != leader)
        .expect("coordinator");
    let lagging = (1..=cfg.num_nodes as NodeId)
        .find(|&pid| pid != leader && pid != coordinator)
        .expect("lagging");
    let value = Value::with_id(1002);

    // Sever coordinator → lagging so lagging never receives the FastPropose.
    // With N=3 and fast_quorum=3, only 2 FastAccepted will reach the leader,
    // which is below the super-quorum threshold.
    sys.nodes
        .get(&coordinator)
        .unwrap()
        .on_definition(|x| x.set_connection(lagging, false));

    coordinator_append(&sys, coordinator, value.clone());

    // The resend timer (default 500 ms) detects the stall and commits via the
    // slow path.  The fallback AcceptDecide ensures the lagging follower also
    // receives the entry and eventually decides it.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &[value.clone()], cfg.wait_timeout);
    }

    print_final_logs(&sys, "happy_path_slow_path");
    test_end("happy_path_slow_path");
    shutdown(sys);
}
