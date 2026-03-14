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

/// Like `inject_fast_propose` but skips `exclude`.
fn inject_fast_propose_except(
    sys: &TestSystem,
    from: NodeId,
    proposal: AcceptDecide<Value>,
    exclude: NodeId,
) {
    for pid in 1..=sys.nodes.len() as NodeId {
        if pid == exclude {
            continue;
        }
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

fn get_dom_hash(sys: &TestSystem, pid: NodeId) -> u64 {
    sys.nodes
        .get(&pid)
        .unwrap()
        .on_definition(|x| x.paxos.get_dom_hash())
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
        let (decided_idx, entries, dom_hash) = sys.nodes.get(&pid).unwrap().on_definition(|x| {
            let decided_idx = x.paxos.get_decided_idx();
            let entries = x.paxos.read_entries(0..decided_idx).unwrap_or_default();
            let dom_hash = x.paxos.get_dom_hash();
            (decided_idx, entries, dom_hash)
        });

        eprintln!(
            "\x1b[35m├─ Node {pid}  (decided_idx={decided_idx}, entries={}, dom_hash={dom_hash:#018x})\x1b[0m",
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

/// Edge case: fast-path 1-RTT coordinator decision arrives before the
/// cluster-wide Decide reaches a lagging node.
///
/// N=7, fast_quorum=6.  The leader→lagging link is severed before the first
/// proposal so the Decide broadcast can never reach lagging.  The coordinator
/// appends a value and immediately receives a fast-path reply (1 RTT), deciding
/// it locally before any Decide has been sent cluster-wide.
///
/// Verified properties:
///   - When the coordinator's fast-path future fires, lagging's decided_idx is
///     still 0 — proving the coordinator decided strictly before the
///     cluster-wide Decide reached lagging.
///   - Lagging has the entry in its log as Undecided (it accepted via fast path
///     but hasn't committed because the Decide is blocked).
///   - After reconnecting lagging and appending a second value, all 7 nodes
///     eventually commit both entries.
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

    // At this exact moment the coordinator has its fast-path 1-RTT reply.
    // The cluster-wide Decide cannot have reached lagging because
    // leader→lagging is blocked, so lagging must still be undecided.
    let lagging_decided_idx = sys
        .nodes
        .get(&lagging)
        .unwrap()
        .on_definition(|x| x.paxos.get_decided_idx());
    assert_eq!(
        lagging_decided_idx, 0,
        "lagging must not have decided yet when coordinator gets its fast-path reply \
         (leader→lagging link is blocked)"
    );

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

/// Edge case: fast quorum not reached, system must fall back to the slow path.
///
/// N=3, fast_quorum=3 (all nodes required).  The coordinator's outgoing link
/// to one follower is severed so that follower never receives the FastPropose.
/// Only 2 FastAccepted messages reach the leader, which is below the super
/// quorum threshold — the fast path cannot complete.
///
/// Verified properties:
///   - The resend timer detects the stall (accepted_idx > decided_idx, regular
///     quorum already satisfied by the 2 connected nodes) and commits via the
///     slow path.
///   - The lagging follower receives the entry via a fallback AcceptDecide and
///     eventually decides it.
///   - After reconnecting, a second value is proposed and all 3 nodes decide
///     both entries, confirming recovery is clean.
#[test]
#[serial]
fn partial_fast_quorum_falls_back_to_slow_path() {
    test_begin("partial_fast_quorum_falls_back_to_slow_path");
    let cfg = dom_default_testcfg(Some(3)); // N=3 so fast_quorum=3; blocking one follower drops below quorum
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

/// Edge case: coordinator crashes after broadcasting FastPropose but before the
/// super quorum can be reached.
///
/// N=3, fast_quorum=3.  The coordinator appends a value (which broadcasts
/// FastPropose to all nodes), then is killed after a short delay.  With only 2
/// surviving nodes, fast_quorum=3 can never be met.
///
/// Verified properties:
///   - The two surviving nodes release the entry from their DOM buffers after
///     the deadline expires, fast-accept it, and send FastAccepted to the
///     leader.
///   - Only 2 FastAccepted arrive (below super quorum) so fast_decide is not
///     triggered.  The resend timer detects the stall, sees that the regular
///     quorum (2/2 surviving nodes) is met, and decides via the slow path.
///   - Both surviving nodes eventually commit the entry, proving the algorithm
///     terminates despite coordinator failure.
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

/// Edge case: a fully partitioned follower's log diverges from the cluster;
/// Phase 1 recovery (AcceptSync) must reconcile it.
///
/// N=7.  One follower is completely isolated while two entries are proposed and
/// decided by the rest of the cluster.  The leader's resend fallback also
/// advances the per-follower seq_num counter for the isolated node even though
/// the messages can't be delivered.
///
/// Verified properties:
///   - After reconnecting the isolated node, a third proposal triggers a
///     DroppedPreceding sequence-number gap which fires Phase 1 recovery
///     (PrepareReq → Prepare → Promise → AcceptSync).
///   - The AcceptSync delivers the missing entries to the isolated node with
///     the correct DOM hash, bringing it fully in sync.
///   - All 7 nodes decide exactly [v1, v2, v3] in that order.  (v1 and v2
///     come from the same coordinator in sequence so their deadlines are
///     strictly ordered.)
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

    // Verify all nodes decided exactly [v1, v2, v3] in that order.
    // ValueSnapshot preserves entry order so read_decided_values handles
    // snapshotted entries correctly.  v1 and v2 are sequential proposals
    // from the same coordinator, giving v1.deadline < v2.deadline, so DOM
    // always releases them in order.
    let expected = vec![v1, v2, v3];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    print_final_logs(&sys, "divergent_logs_reconcile_via_slow_path");
    test_end("divergent_logs_reconcile_via_slow_path");
    shutdown(sys);
}

/// Edge case: two concurrent fast-path proposals carry an identical deadline;
/// the tie-break rule must produce a deterministic, globally-agreed log order.
///
/// N=7, fast_quorum=6.  Two coordinators (pids 2 and 3) simultaneously inject
/// a FastPropose to all 7 nodes.  Both proposals use `shared_deadline`, so the
/// DOM `early_buffer` (a min-heap on deadline) holds them with equal priority.
///
/// Tie-break semantics (`AcceptDecide::Ord`):
///   The heap is a min-heap on deadline, so equal deadlines use the secondary
///   comparator `self.id.cmp(&other.id)` in ascending order — meaning the entry
///   with the **larger** `(coordinator_id, request_id)` tuple is treated as
///   "smaller" in the heap (i.e. it is popped first).
///   Here `id=(3, 300) > id=(2, 200)`, so coordinator 3's proposal (value 30)
///   is released before coordinator 2's proposal (value 20).
///
/// Verified properties:
///   - All 7 nodes release the two entries in the same order: [30, 20].
///   - The final decided log on every node is exactly `[Value(30), Value(20)]`,
///     confirming a consistent global order from the tie-break rule.
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
        dom_hash: 0,
    };
    let second = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(30)],
        deadline: shared_deadline,
        id: (3, 300),
        dom_hash: 0,
    };

    inject_fast_propose(&sys, 2, first);
    inject_fast_propose(&sys, 3, second);

    let expected = vec![Value::with_id(30), Value::with_id(20)];
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
        dom_hash: 0,
    };
    let proposal_b = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(20)],
        deadline: now + 100_000,
        id: (3, 2000),
        dom_hash: 0,
    };
    let proposal_c = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(30)],
        deadline: now + 150_000,
        id: (4, 3000),
        dom_hash: 0,
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
/// The straggler is chosen as the lowest-pid non-leader so that it cannot win
/// BLE re-election after reconnection (BLE breaks ties by pid; the highest pid
/// wins, so a low-pid straggler remains a follower).
///
/// After reconnecting the straggler a fourth proposal is injected.  The
/// straggler's `accepted_idx=0` is behind the FastPropose's `decided_idx=3` →
/// stale-log guard fires → `reconnected()` → Phase 1 recovery delivers all
/// four entries via AcceptSync, bringing all seven nodes into agreement.
#[test]
#[serial]
fn seven_nodes_multiple_coordinators_straggler_recovers() {
    test_begin("seven_nodes_multiple_coordinators_straggler_recovers");
    let cfg = dom_default_testcfg(None);
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Pick the straggler as the lowest-pid non-leader node so it can never
    // win a BLE re-election (BLE breaks ballot ties by pid; the highest pid
    // always wins, so a low-pid straggler stays a follower after reconnection).
    let straggler: NodeId = (1..=cfg.num_nodes as NodeId)
        .find(|&pid| pid != leader)
        .expect("straggler");
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
    // participates in the fast path.  The straggler's decided_idx=0 is behind
    // the FastPropose's decided_idx=3 → stale-log guard fires → reconnected()
    // → Phase 1 recovery delivers all four entries via AcceptSync.
    sys.set_node_connections(straggler, true);

    let (kprom, kfuture) = promise::<()>();
    sys.nodes.get(&c1).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom, v4.clone()));
        x.paxos.append(v4.clone()).expect("append should succeed");
    });
    kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("v4 did not decide in time");

    // After Phase 1 recovery every node must have all four entries committed.
    // v1/v2/v3 were proposed concurrently so their log order is determined by
    // DOM deadlines and is non-deterministic.  v4 is always last (proposed
    // after the first three were already decided).  We therefore verify the
    // set of decided values rather than their exact sequence.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 4
            })
        });
        let decided = read_decided_values(&sys, pid);
        assert_eq!(decided.len(), 4, "node {pid} should have exactly 4 decided entries");
        assert!(decided.contains(&v1), "node {pid} missing v1");
        assert!(decided.contains(&v2), "node {pid} missing v2");
        assert!(decided.contains(&v3), "node {pid} missing v3");
        assert!(decided.contains(&v4), "node {pid} missing v4");
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

/// Test — N=7, 3 coordinators, one follower misses one coordinator's FastPropose.
///
/// With N=7 the fast quorum is 6.  The proposals are staged so isolated never
/// has a chance to accept an entry at the wrong log position:
///
///   Step 1 — c1 injects T1 to 6 nodes (not isolated).
///             6 out of 7 FastAccepted = fast quorum → T1 fast-decides.
///             The Decide for T1 arrives at isolated (leader→isolated is open).
///             isolated's accepted_idx=0 < decided_idx=1 → recovery.
///             AcceptSync sends [T1] from sync_idx=0 → isolated decides T1. ✓
///
///   Step 2 — After isolated has decided T1, c2 and c3 inject T2 and T3 to
///             all 7 nodes with strictly increasing deadlines.  All 7 nodes
///             are in Accept phase and accept each entry at the correct index.
///
/// Expected outcome: all 7 nodes decide [T1, T2, T3] in deadline order.
#[test]
#[serial]
fn seven_nodes_isolated_from_one_coordinator_converges() {
    test_begin("seven_nodes_isolated_from_one_coordinator_converges");
    let cfg = dom_default_testcfg(None); // 7 nodes, fast_quorum=6
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());

    let mut non_leaders: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .collect();
    let c1 = non_leaders.remove(0);
    let c2 = non_leaders.remove(0);
    let c3 = non_leaders.remove(0);
    let isolated = non_leaders.remove(0);

    // ── Step 1: c1 proposes T1 to 6 nodes only ───────────────────────────────
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;

    let p_c1 = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(3100)],
        deadline: now + 50_000,
        id: (c1 as u64, 3100),
        dom_hash: 0,
    };
    // 6 out of 7 nodes receive c1's FastPropose → fast quorum (6) met.
    inject_fast_propose_except(&sys, c1, p_c1, isolated);

    // Wait for isolated to detect the gap and recover.
    // The Decide for T1 arrives at isolated from the leader (leader→isolated is
    // never blocked).  decided_idx=1 > accepted_idx=0 → reconnected() →
    // AcceptSync sends [T1] → isolated decides T1.
    wait_until(cfg.wait_timeout, || {
        sys.nodes
            .get(&isolated)
            .unwrap()
            .on_definition(|x| x.paxos.get_decided_idx() >= 1)
    });

    // ── Step 2: c2 and c3 propose T2 and T3 to all 7 nodes ───────────────────
    // Now that isolated has T1 at idx=0, it can accept T2 and T3 at idx=1 and 2.
    let now2 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;

    let p_c2 = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 1, // T1 is already decided
        entries: vec![Value::with_id(3101)],
        deadline: now2 + 50_000,
        id: (c2 as u64, 3101),
        dom_hash: 0,
    };
    let p_c3 = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 1,
        entries: vec![Value::with_id(3102)],
        deadline: now2 + 100_000,
        id: (c3 as u64, 3102),
        dom_hash: 0,
    };
    inject_fast_propose(&sys, c2, p_c2);
    inject_fast_propose(&sys, c3, p_c3);

    // All 7 nodes must converge to the same three entries in deadline order.
    let expected = vec![
        Value::with_id(3100),
        Value::with_id(3101),
        Value::with_id(3102),
    ];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    print_final_logs(&sys, "seven_nodes_isolated_from_one_coordinator_converges");
    test_end("seven_nodes_isolated_from_one_coordinator_converges");
    shutdown(sys);
}

/// Test — Termination when the leader receives fewer FastAccepted than the super quorum.
///
/// With N=7 and fast_quorum=6, blocking two followers from receiving the
/// coordinator's FastPropose means only 5 FastAccepted reach the leader.
/// The fast path cannot complete.
///
/// The resend timer must detect the stall and fall back to the slow path:
/// regular quorum (4/7) is satisfied by the 5 nodes that did accept, so the
/// leader decides via AcceptDecide, then pushes fallback AcceptDecide messages
/// to the two blocked followers.
///
/// This test verifies the algorithm always terminates — it never hangs waiting
/// for a fast quorum that can no longer be reached.
#[test]
#[serial]
fn termination_when_below_fast_quorum() {
    test_begin("termination_when_below_fast_quorum");
    let cfg = dom_default_testcfg(None); // 7 nodes, fast_quorum=6
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Need two followers that are NOT the leader so we can block exactly 2
    // nodes from receiving the FastPropose.
    let mut non_leaders: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .collect();
    let coordinator = non_leaders.remove(0);
    let blocked1 = non_leaders.remove(0);
    let blocked2 = non_leaders.remove(0);

    let value = Value::with_id(3200);

    // Block coordinator → blocked1 and coordinator → blocked2 so those two
    // nodes never receive the FastPropose.  5 remaining nodes (including the
    // leader) will send FastAccepted → 5 < fast_quorum=6 → fast path stalls.
    sys.nodes.get(&coordinator).unwrap().on_definition(|x| {
        x.set_connection(blocked1, false);
        x.set_connection(blocked2, false);
    });

    coordinator_append(&sys, coordinator, value.clone());

    // The resend timer fires (regular quorum 4/7 is met by the 5 nodes that
    // accepted) → slow-path decide → fallback AcceptDecide to blocked1 and
    // blocked2 via the leader.  All 7 nodes must eventually decide.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &[value.clone()], cfg.wait_timeout);
    }

    print_final_logs(&sys, "termination_when_below_fast_quorum");
    test_end("termination_when_below_fast_quorum");
    shutdown(sys);
}

/// Test — DOM hash diverges after a slow-path decision when one node missed
/// the fast-path window.
///
/// Scenario (N=3, fast_quorum=3):
///   - coordinator proposes entry T
///   - leader and coordinator fast-accept T → their `dom.last_log_hash` is
///     updated via `record_accepted_metadata`
///   - isolated never received the FastPropose → doesn't fast-accept T
///   - Only 2 FastAccepted reach the leader (< fast_quorum=3) → slow-path
///     fallback: Decide(hash=0) + fallback AcceptDecide to isolated
///   - isolated receives T via slow-path AcceptDecide (fallback from `resend_messages_leader`)
///     → appends T and adopts the leader's dom_hash from the AcceptDecide message
///
/// After T is decided by all three nodes, all hashes must agree:
///   - leader.dom_hash      = H(T_metadata)  [fast-accepted]
///   - coordinator.dom_hash = H(T_metadata)  [same]
///   - isolated.dom_hash    = H(T_metadata)  [adopted from slow-path AcceptDecide]
///
/// This verifies the fix: `dom_hash` is piggy-backed on AcceptDecide and AcceptSync
/// so slow-path recipients end up with the same hash as fast-path acceptors.
#[test]
#[serial]
fn dom_hash_diverges_after_slow_path_decision() {
    test_begin("dom_hash_diverges_after_slow_path_decision");
    let cfg = dom_default_testcfg(Some(3)); // N=3, fast_quorum=3 (ALL nodes)
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = (1..=cfg.num_nodes as NodeId)
        .find(|&p| p != leader)
        .expect("coordinator");
    let isolated = (1..=cfg.num_nodes as NodeId)
        .find(|&p| p != leader && p != coordinator)
        .expect("isolated");

    let value = Value::with_id(3300);

    // Block coordinator → isolated so isolated never receives the FastPropose.
    // With fast_quorum=3 (all nodes), only 2 FA reach the leader →
    // fast path cannot complete → resend timer triggers slow-path fallback.
    sys.nodes
        .get(&coordinator)
        .unwrap()
        .on_definition(|x| x.set_connection(isolated, false));

    coordinator_append(&sys, coordinator, value.clone());

    // Wait for all three nodes to decide T via the slow path.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &[value.clone()], cfg.wait_timeout);
    }

    // Capture hashes after the slow-path round.
    let leader_hash = get_dom_hash(&sys, leader);
    let coordinator_hash = get_dom_hash(&sys, coordinator);
    let isolated_hash = get_dom_hash(&sys, isolated);

    eprintln!(
        "  leader({leader})      dom_hash = {leader_hash:#018x}\n  \
         coordinator({coordinator}) dom_hash = {coordinator_hash:#018x}\n  \
         isolated({isolated})    dom_hash = {isolated_hash:#018x}"
    );

    // Leader and coordinator both fast-accepted T — their hashes must agree.
    assert_eq!(
        leader_hash, coordinator_hash,
        "leader and coordinator both fast-accepted T; their hashes must match"
    );

    // Isolated received T via slow-path AcceptDecide which now carries
    // dom_hash.  After adopting it, all three nodes must agree.
    assert_eq!(
        leader_hash, isolated_hash,
        "isolated slow-path accepted T; after adopting dom_hash from \
         AcceptDecide it must match the leader's hash"
    );

    print_final_logs(&sys, "dom_hash_diverges_after_slow_path_decision");
    test_end("dom_hash_diverges_after_slow_path_decision");
    shutdown(sys);
}

/// Edge case: the leader receives a FastPropose whose deadline has already
/// passed relative to its own `last_released_timestamp` (stale deadline).
///
/// Setup (N=3, fast_quorum=3):
///   Step 1 — a warmup entry is injected to all nodes with a known deadline
///             D_warmup.  All three nodes fast-accept it, the fast quorum is
///             met, and every node decides it.  After this step every node's
///             `last_released_timestamp = D_warmup`.
///
///   Step 2 — a second entry is injected with `stale_deadline = D_warmup - 1`
///             (one microsecond before the last release):
///               * Followers use `handle_fast_propose` (no reordering): since
///                 `stale_deadline <= last_released_timestamp`, the entry goes
///                 into the `late_buffer` and is never fast-accepted.
///               * The leader uses `handle_fast_propose_leader`: it detects the
///                 stale deadline and bumps it to `last_released_timestamp + 1`
///                 before inserting into `early_buffer`.
///
/// Verified properties:
///   - The leader eventually releases the reordered entry, appends it to the
///     log, and — since no fast quorum can be reached (followers never
///     fast-accepted it) — the resend timer falls back to the slow path.
///   - The fallback AcceptDecide carries the leader's `dom_hash`, which
///     followers adopt on acceptance.
///   - All 3 nodes decide both entries in order; their DOM hashes agree.
#[test]
#[serial]
fn leader_reorders_stale_deadline_decides_via_slow_path() {
    test_begin("leader_reorders_stale_deadline_decides_via_slow_path");
    let cfg = dom_default_testcfg(Some(3));
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = (1..=cfg.num_nodes as NodeId)
        .find(|&p| p != leader)
        .expect("coordinator");
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;

    // ── Step 1: decide a warmup entry via normal fast path ────────────────────
    // All nodes receive the proposal with d_warmup.  Because d_warmup > LRT=0
    // on every node, they all put it in their early_buffer.  All three
    // fast-accept it, the super quorum (3/3) is met, and the entry is decided.
    // After this every node has last_released_timestamp = d_warmup.
    let d_warmup = now + 50_000; // 50 ms from now
    let v_warmup = Value::with_id(5000);
    let warmup_proposal = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![v_warmup.clone()],
        deadline: d_warmup,
        id: (coordinator as u64, 5000),
        dom_hash: 0,
    };
    inject_fast_propose(&sys, coordinator, warmup_proposal);
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &[v_warmup.clone()], cfg.wait_timeout);
    }

    // ── Step 2: inject the "late" entry ───────────────────────────────────────
    // stale_deadline < d_warmup = last_released_timestamp on all nodes.
    // Followers (handle_fast_propose): stale_deadline <= LRT → late_buffer.
    // Leader  (handle_fast_propose_leader): stale_deadline <= LRT → reorder
    //         to LRT+1, insert into early_buffer.
    let stale_deadline = d_warmup - 1;
    let v_late = Value::with_id(5001);
    let late_proposal = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 1, // warmup is decided
        entries: vec![v_late.clone()],
        deadline: stale_deadline,
        id: (coordinator as u64, 5001),
        dom_hash: 0,
    };
    inject_fast_propose(&sys, coordinator, late_proposal);

    // The leader reorders and eventually releases the entry; followers never
    // fast-accept it (it is in their late_buffer).  The resend timer detects
    // the stall (accepted_idx > decided_idx, quorum not met) and falls back to
    // the slow path: a fallback AcceptDecide is sent to the followers.
    // All three nodes must eventually decide both entries.
    let expected = vec![v_warmup, v_late];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    // After the slow-path fallback the leader's dom_hash is propagated to
    // followers via the AcceptDecide's dom_hash field; all three must agree.
    let hashes: Vec<u64> = (1..=cfg.num_nodes as NodeId)
        .map(|p| get_dom_hash(&sys, p))
        .collect();
    assert!(
        hashes.windows(2).all(|w| w[0] == w[1]),
        "DOM hashes must agree after slow-path fallback for reordered entry: {:?}",
        hashes,
    );

    print_final_logs(&sys, "leader_reorders_stale_deadline_decides_via_slow_path");
    test_end("leader_reorders_stale_deadline_decides_via_slow_path");
    shutdown(sys);
}

/// Edge case: a FastPropose arrives at the leader with a different deadline
/// than the version that followers received, causing a hash mismatch on the
/// leader's `FastAccepted` tracker.
///
/// Setup (N=3, fast_quorum=3):
///   The same entry (same `id`) is injected with two different deadlines:
///     - D1 (small) → sent to all **followers** only.  Each follower puts it
///       in its `early_buffer`, releases it when D1 expires, fast-accepts, and
///       sends `FastAccepted { hash=H1 }` to the leader.
///     - D2 >> D1 → sent to the **leader** only.  The leader puts it in its
///       `early_buffer`, releases it later (when D2 expires), appends it, and
///       computes `H2 ≠ H1` (different deadline → different DomMetadata).
///
/// Hash-mismatch correction path:
///   The followers' FastAccepted messages (hash=H1) arrive at the leader before
///   the leader releases its own entry (because D1 fires first).  When the
///   leader subsequently processes its own entry with H2, it calls
///   `dom.handle_fast_accepted` which returns `HashMismatch` (H2 ≠ H1 = qd.hash).
///   The leader identifies all followers in the quorum tracker (they all used
///   H1) and sends each a `Decide` carrying H2.  Each follower's `handle_decide`
///   directly adopts H2 as its `dom.last_log_hash` (no Phase 1 recovery needed —
///   Paxos guarantees the log entries are correct; only the deadline-derived hash
///   metadata needs updating).  The stall-recovery path then decides the entry
///   via the slow path and all nodes converge.
///
/// Verified properties:
///   - All 3 nodes decide the entry.
///   - All 3 nodes have the same `dom.last_log_hash` after convergence (= H2,
///     the leader's authoritative hash).
#[test]
#[serial]
fn hash_mismatch_on_fast_accepted_triggers_recovery() {
    test_begin("hash_mismatch_on_fast_accepted_triggers_recovery");
    let cfg = dom_default_testcfg(Some(3));
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let coordinator = (1..=cfg.num_nodes as NodeId)
        .find(|&p| p != leader)
        .expect("coordinator");
    let ballot = sys
        .nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.get_promise());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time is broken")
        .as_micros() as i64;

    // D1: deadline given to followers  — fires in ~50 ms.
    // D2: deadline given to the leader — fires in ~300 ms.
    // D1 fires first so followers fast-accept with H1 and their FastAccepted
    // reach the leader before the leader releases its own copy (at D2).
    let d1 = now + 50_000;
    let d2 = now + 300_000;

    let value = Value::with_id(5100);
    let entry_id = (coordinator as u64, 5100u64);

    let proposal_followers = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![value.clone()],
        deadline: d1,
        id: entry_id,
        dom_hash: 0,
    };
    let proposal_leader = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![value.clone()],
        deadline: d2,
        id: entry_id,
        dom_hash: 0,
    };

    // Broadcast D1 to every node except the leader.
    inject_fast_propose_except(&sys, coordinator, proposal_followers, leader);

    // Deliver D2 only to the leader (simulating the leader's reordered copy).
    let leader_msg = Message::SequencePaxos(PaxosMessage {
        from: coordinator,
        to: leader,
        msg: PaxosMsg::FastPropose(proposal_leader),
    });
    sys.nodes
        .get(&leader)
        .unwrap()
        .on_definition(|x| x.paxos.handle_incoming(leader_msg));

    // Followers release at D1 (hash H1) and send FastAccepted{H1} to the leader.
    // The leader releases at D2 (hash H2 ≠ H1).  handle_fast_accepted returns
    // HashMismatch → proactive Decide{H2} sent to each follower →
    // handle_decide adopts H2 directly (no Phase 1 needed) →
    // stall recovery decides via slow path → all nodes decide.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &[value.clone()], cfg.wait_timeout);
    }

    // After AcceptSync, every node's dom.last_log_hash must equal the leader's.
    let leader_hash = get_dom_hash(&sys, leader);
    for pid in 1..=cfg.num_nodes as NodeId {
        let h = get_dom_hash(&sys, pid);
        assert_eq!(
            h, leader_hash,
            "node {pid} dom_hash={h:#018x} != leader_hash={leader_hash:#018x} \
             after hash-mismatch recovery"
        );
    }

    print_final_logs(&sys, "hash_mismatch_on_fast_accepted_triggers_recovery");
    test_end("hash_mismatch_on_fast_accepted_triggers_recovery");
    shutdown(sys);
}
