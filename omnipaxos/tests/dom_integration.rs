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

/// RAII guard that prints the final log dump and shuts down the Kompact system
/// when dropped — including on panic.  This ensures the log is always visible
/// even when a test assertion fires mid-test.
struct TestGuard {
    sys: Option<TestSystem>,
    name: &'static str,
}

impl TestGuard {
    fn new(sys: TestSystem, name: &'static str) -> Self {
        Self { sys: Some(sys), name }
    }
}

impl std::ops::Deref for TestGuard {
    type Target = TestSystem;
    fn deref(&self) -> &TestSystem {
        self.sys.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for TestGuard {
    fn deref_mut(&mut self) -> &mut TestSystem {
        self.sys.as_mut().unwrap()
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        if let Some(sys) = &self.sys {
            print_final_logs(sys, self.name);
        }
        if let Some(mut sys) = self.sys.take() {
            if let Some(ks) = std::mem::take(&mut sys.kompact_system) {
                // Don't panic in Drop — a double-panic causes an abort.
                if let Err(e) = ks.shutdown() {
                    eprintln!("Error on kompact shutdown: {e}");
                }
            }
        }
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

fn dom_default_testcfg(num_nodes: Option<usize>, wait_timeout: Option<Duration>) -> TestConfig {
    let num_nodes = num_nodes.unwrap_or(7);
    let wait_timeout = wait_timeout.unwrap_or(Duration::from_millis(10_000));
    TestConfig {
        num_nodes,
        num_threads: num_nodes,
        wait_timeout,
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
    let cfg = dom_default_testcfg(None, None);
    let sys = TestGuard::new(TestSystem::with(cfg), "fast_path_coordinator_decides_before_cluster_wide_decide");
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

    test_end("fast_path_coordinator_decides_before_cluster_wide_decide");
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
    let cfg = dom_default_testcfg(Some(3), None); // N=3 so fast_quorum=3; blocking one follower drops below quorum
    let sys = TestGuard::new(TestSystem::with(cfg), "partial_fast_quorum_falls_back_to_slow_path");
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

    test_end("partial_fast_quorum_falls_back_to_slow_path");
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
    let cfg = dom_default_testcfg(Some(3), None);
    let mut sys = TestGuard::new(TestSystem::with(cfg), "coordinator_crash_still_decides");
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

    test_end("coordinator_crash_still_decides");
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
    let cfg = dom_default_testcfg(None, None);
    let sys = TestGuard::new(TestSystem::with(cfg), "divergent_logs_reconcile_via_slow_path");
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

    // v3 is always last (appended after v1/v2 are decided).  v1 and v2 may
    // appear in either order depending on which deadline the simulated clock
    // assigned: a backward clock jump between the two proposals can give
    // v2.deadline < v1.deadline, causing DOM to release v2 first.  All nodes
    // must converge to the same order — read it from the coordinator as the
    // reference.
    let reference: Vec<Value> = {
        let mut r = Vec::new();
        wait_until(cfg.wait_timeout, || {
            r = read_decided_values(&sys, coordinator);
            r.len() >= 3
        });
        r
    };
    assert_eq!(reference[2], v3, "v3 must be the third decided value");
    assert!(
        (reference[0] == v1 && reference[1] == v2)
            || (reference[0] == v2 && reference[1] == v1),
        "first two decided values must be {{v1, v2}} in some order, got {:?}",
        &reference[..2]
    );
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &reference, cfg.wait_timeout);
    }

    test_end("divergent_logs_reconcile_via_slow_path");
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
    let cfg = dom_default_testcfg(None, None);
    let sys = TestGuard::new(TestSystem::with(cfg), "fast_path_same_deadline_tiebreaks_by_coordinator_pid");
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
        prev_idx: 0,
    };
    let second = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(30)],
        deadline: shared_deadline,
        id: (3, 300),
        dom_hash: 0,
        prev_idx: 0,
    };

    inject_fast_propose(&sys, 2, first);
    inject_fast_propose(&sys, 3, second);

    let expected = vec![Value::with_id(30), Value::with_id(20)];
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    test_end("fast_path_same_deadline_tiebreaks_by_coordinator_pid");
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
    let cfg = dom_default_testcfg(None, None);
    let sys = TestGuard::new(TestSystem::with(cfg), "seven_nodes_three_coordinators_deadline_ordering");
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
        prev_idx: 0,
    };
    let proposal_b = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(20)],
        deadline: now + 100_000,
        id: (3, 2000),
        dom_hash: 0,
        prev_idx: 0,
    };
    let proposal_c = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![Value::with_id(30)],
        deadline: now + 150_000,
        id: (4, 3000),
        dom_hash: 0,
        prev_idx: 0,
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

    test_end("seven_nodes_three_coordinators_deadline_ordering");
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
    let cfg = dom_default_testcfg(None, None);
    let sys = TestGuard::new(TestSystem::with(cfg), "seven_nodes_multiple_coordinators_straggler_recovers");
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

    test_end("seven_nodes_multiple_coordinators_straggler_recovers");
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
    let cfg = dom_default_testcfg(Some(3), None); // 3 nodes, all connected
    let sys = TestGuard::new(TestSystem::with(cfg), "happy_path_fast_path");
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

    test_end("happy_path_fast_path");
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
    let cfg = dom_default_testcfg(Some(3), None); // 3 nodes
    let sys = TestGuard::new(TestSystem::with(cfg), "happy_path_slow_path");
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

    test_end("happy_path_slow_path");
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
    let cfg = dom_default_testcfg(None, None); // 7 nodes, fast_quorum=6
    let sys = TestGuard::new(TestSystem::with(cfg), "seven_nodes_isolated_from_one_coordinator_converges");
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
        prev_idx: 0,
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
        prev_idx: 0,
    };
    let p_c3 = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 1,
        entries: vec![Value::with_id(3102)],
        deadline: now2 + 100_000,
        id: (c3 as u64, 3102),
        dom_hash: 0,
        prev_idx: 0,
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

    test_end("seven_nodes_isolated_from_one_coordinator_converges");
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
    let cfg = dom_default_testcfg(None, None); // 7 nodes, fast_quorum=6
    let sys = TestGuard::new(TestSystem::with(cfg), "termination_when_below_fast_quorum");
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

    test_end("termination_when_below_fast_quorum");
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
    let cfg = dom_default_testcfg(Some(3), None); // N=3, fast_quorum=3 (ALL nodes)
    let sys = TestGuard::new(TestSystem::with(cfg), "dom_hash_diverges_after_slow_path_decision");
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

    test_end("dom_hash_diverges_after_slow_path_decision");
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
    let cfg = dom_default_testcfg(Some(3), None);
    let sys = TestGuard::new(TestSystem::with(cfg), "leader_reorders_stale_deadline_decides_via_slow_path");
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
        prev_idx: 0,
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
        prev_idx: 0,
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

    test_end("leader_reorders_stale_deadline_decides_via_slow_path");
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
    let cfg = dom_default_testcfg(Some(3), None);
    let sys = TestGuard::new(TestSystem::with(cfg), "hash_mismatch_on_fast_accepted_triggers_recovery");
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
        prev_idx: 0,
    };
    let proposal_leader = AcceptDecide {
        n: ballot,
        seq_num: SequenceNumber::default(),
        decided_idx: 0,
        entries: vec![value.clone()],
        deadline: d2,
        id: entry_id,
        dom_hash: 0,
        prev_idx: 0,
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

    test_end("hash_mismatch_on_fast_accepted_triggers_recovery");
}

/// Fault tolerance — leader crashes while fast-path entries are in-flight.
///
/// N=7, fast_quorum=6.  Three coordinators each submit an entry.  After a
/// short propagation window the current leader is killed before the resend
/// timer has had a chance to fire slow-path fallback for all three entries.
///
/// The remaining 6 nodes must:
///   1. Detect the missing leader heartbeats (BLE timeout).
///   2. Elect a new leader.
///   3. Run Phase 1 (Prepare/Promise) to discover any accepted-but-undecided
///      entries left by the crashed leader.
///   4. Deliver those entries to every node via AcceptSync/AcceptDecide.
///   5. Decide all 3 entries on all 6 surviving nodes.
///
/// This validates that the safety property "every accepted entry is eventually
/// decided" holds even when the leader fails mid-stream.
#[test]
#[serial]
fn leader_crash_pending_fast_path_entries_decided() {
    test_begin("leader_crash_pending_fast_path_entries_decided");
    let cfg = dom_default_testcfg(None, None); // N=7
    let mut sys = TestGuard::new(
        TestSystem::with(cfg),
        "leader_crash_pending_fast_path_entries_decided",
    );
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Wait for all nodes to reach Phase::Accept so proposals go via fast path.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((_, true)))
            })
        });
    }

    // Pick 3 non-leader coordinators.
    let mut non_leaders: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .take(3)
        .collect();
    let c1 = non_leaders.remove(0);
    let c2 = non_leaders.remove(0);
    let c3 = non_leaders.remove(0);

    let v1 = Value::with_id(4000);
    let v2 = Value::with_id(4001);
    let v3 = Value::with_id(4002);

    // Submit all three entries.
    for (coord, val) in [(c1, v1.clone()), (c2, v2.clone()), (c3, v3.clone())] {
        sys.nodes.get(&coord).unwrap().on_definition(|x| {
            x.paxos.append(val).expect("append should succeed");
        });
    }

    // Let FastPropose messages propagate before killing the leader.
    sleep(Duration::from_millis(50));
    sys.kill_node(leader);

    // The 6 remaining nodes run BLE → new leader elected → Phase 1 recovery
    // → AcceptSync delivers all pending entries → all 6 decide.
    let active: Vec<NodeId> = sys.nodes.keys().copied().collect();
    for pid in &active {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 3
            })
        });
        let decided = read_decided_values(&sys, *pid);
        assert_eq!(decided.len(), 3, "node {pid} should have 3 decided entries");
        assert!(decided.contains(&v1), "node {pid} missing v1");
        assert!(decided.contains(&v2), "node {pid} missing v2");
        assert!(decided.contains(&v3), "node {pid} missing v3");
    }

    test_end("leader_crash_pending_fast_path_entries_decided");
}

/// Fault tolerance — rolling leader failures: the cluster elects two new
/// leaders in sequence and continues making progress after each failure.
///
/// N=7.  The test proceeds in three epochs:
///
///   Epoch 1: all 7 nodes.  Submit E1 via C1, wait for all 7 to decide.
///            Then kill leader L1 → 6 nodes remain.
///
///   Epoch 2: 6 nodes.  New leader L2 elected.  Submit E2 via a surviving
///            coordinator, wait for all 6 to decide.
///            Then kill L2 → 5 nodes remain.
///
///   Epoch 3: 5 nodes.  New leader L3 elected.  Submit E3 via a surviving
///            coordinator, wait for all 5 to decide.
///
/// At the end every surviving node must have decided [E1, E2, E3] (exact
/// content, in submission order — each entry is submitted only after the
/// previous one is fully decided so the order is deterministic).
///
/// Majority quorum for N=7 is 4.  With 5 nodes remaining in epoch 3 there
/// is sufficient overlap to maintain safety and liveness.
#[test]
#[serial]
fn rolling_leader_failures_cluster_survives() {
    test_begin("rolling_leader_failures_cluster_survives");
    let cfg = dom_default_testcfg(None, None); // N=7
    let mut sys = TestGuard::new(
        TestSystem::with(cfg),
        "rolling_leader_failures_cluster_survives",
    );
    sys.start_all_nodes();

    // ── Epoch 1: all 7 nodes ─────────────────────────────────────────────────
    let l1 = sys.get_elected_leader(1, cfg.wait_timeout);
    // Wait for accept phase on all nodes.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((_, true)))
            })
        });
    }
    let c1 = (1..=cfg.num_nodes as NodeId)
        .find(|&p| p != l1)
        .expect("c1");
    let e1 = Value::with_id(4100);
    let (kp1, kf1) = promise::<()>();
    sys.nodes.get(&c1).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kp1, e1.clone()));
        x.paxos.append(e1.clone()).expect("append e1");
    });
    kf1.wait_timeout(cfg.wait_timeout).expect("e1 not decided");
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_for_decided_values(&sys, pid, &[e1.clone()], cfg.wait_timeout);
    }

    sys.kill_node(l1);

    // ── Epoch 2: 6 nodes ─────────────────────────────────────────────────────
    // Poll until any surviving node sees a new leader that is not l1.
    let observer2 = *sys.nodes.keys().next().unwrap();
    wait_until(cfg.wait_timeout, || {
        sys.nodes.get(&observer2).unwrap().on_definition(|x| {
            matches!(x.paxos.get_current_leader(), Some((ldr, _)) if ldr != l1)
        })
    });
    let l2 = sys.nodes.get(&observer2).unwrap().on_definition(|x| {
        x.paxos.get_current_leader().map(|(ldr, _)| ldr).unwrap()
    });
    // Wait for all surviving nodes to reach Phase::Accept under l2.
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((ldr, true)) if ldr == l2)
            })
        });
    }
    let c2 = sys
        .nodes
        .keys()
        .copied()
        .find(|&p| p != l2)
        .expect("c2");
    let e2 = Value::with_id(4101);
    let (kp2, kf2) = promise::<()>();
    sys.nodes.get(&c2).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kp2, e2.clone()));
        x.paxos.append(e2.clone()).expect("append e2");
    });
    kf2.wait_timeout(cfg.wait_timeout).expect("e2 not decided");
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_until(cfg.wait_timeout, || {
            read_decided_values(&sys, pid).len() >= 2
        });
    }

    sys.kill_node(l2);

    // ── Epoch 3: 5 nodes ─────────────────────────────────────────────────────
    let observer3 = *sys.nodes.keys().next().unwrap();
    wait_until(cfg.wait_timeout, || {
        sys.nodes.get(&observer3).unwrap().on_definition(|x| {
            matches!(x.paxos.get_current_leader(), Some((ldr, _)) if ldr != l1 && ldr != l2)
        })
    });
    let l3 = sys.nodes.get(&observer3).unwrap().on_definition(|x| {
        x.paxos.get_current_leader().map(|(ldr, _)| ldr).unwrap()
    });
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((ldr, true)) if ldr == l3)
            })
        });
    }
    let c3 = sys
        .nodes
        .keys()
        .copied()
        .find(|&p| p != l3)
        .expect("c3");
    let e3 = Value::with_id(4102);
    let (kp3, kf3) = promise::<()>();
    sys.nodes.get(&c3).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kp3, e3.clone()));
        x.paxos.append(e3.clone()).expect("append e3");
    });
    kf3.wait_timeout(cfg.wait_timeout).expect("e3 not decided");

    // All 5 surviving nodes must have decided all three entries.
    // Exact order: e1 then e2 then e3 (each submitted after the previous decided).
    let expected = vec![e1, e2, e3];
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_for_decided_values(&sys, pid, &expected, cfg.wait_timeout);
    }

    test_end("rolling_leader_failures_cluster_survives");
}

/// Fault tolerance — all coordinators crash after submitting; the leader and
/// remaining followers decide via the slow path with a reduced live set.
///
/// N=7, fast_quorum=6.  Three non-leader coordinators each submit an entry,
/// then all three are killed after a propagation window.  Only 4 nodes remain
/// (the leader + 3 other followers).  The fast quorum of 6 can never be
/// reached, but the majority quorum (4/7) is still satisfied by the 4
/// survivors.
///
/// Expected outcome:
///   - The DOM deadline expires on the 4 surviving nodes; they release the
///     entries from their early buffers and send FastAccepted to the leader.
///   - Only 4 FastAccepted arrive (< fast_quorum=6) → fast path stalls.
///   - The resend timer detects the stall (regular quorum = 4 ≥ majority = 4)
///     and decides via the slow path.
///   - All 4 surviving nodes commit all 3 entries.
#[test]
#[serial]
fn all_coordinators_crash_slow_path_decides() {
    test_begin("all_coordinators_crash_slow_path_decides");
    let cfg = dom_default_testcfg(None, None); // N=7
    let mut sys = TestGuard::new(
        TestSystem::with(cfg),
        "all_coordinators_crash_slow_path_decides",
    );
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Wait for all nodes to reach Phase::Accept.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((_, true)))
            })
        });
    }

    // Pick 3 non-leader coordinators to kill.
    let doomed: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .take(3)
        .collect();

    let v1 = Value::with_id(4200);
    let v2 = Value::with_id(4201);
    let v3 = Value::with_id(4202);

    for (coord, val) in doomed.iter().copied().zip([v1.clone(), v2.clone(), v3.clone()]) {
        sys.nodes.get(&coord).unwrap().on_definition(|x| {
            x.paxos.append(val).expect("append should succeed");
        });
    }

    // Let FastPropose reach all 7 nodes before killing the coordinators.
    sleep(Duration::from_millis(60));
    for coord in &doomed {
        sys.kill_node(*coord);
    }

    // The 4 surviving nodes release entries from DOM buffers, send
    // FastAccepted to the leader (4 < fast_quorum=6 → stall), then the
    // resend timer fires the slow-path fallback.  Regular quorum 4/7 is met.
    let survivors: Vec<NodeId> = sys.nodes.keys().copied().collect();
    assert_eq!(survivors.len(), 4, "expected 4 surviving nodes");
    for pid in &survivors {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 3
            })
        });
        let decided = read_decided_values(&sys, *pid);
        assert_eq!(decided.len(), 3, "node {pid} should have 3 decided entries");
        assert!(decided.contains(&v1), "node {pid} missing v1");
        assert!(decided.contains(&v2), "node {pid} missing v2");
        assert!(decided.contains(&v3), "node {pid} missing v3");
    }

    test_end("all_coordinators_crash_slow_path_decides");
}

/// Fault tolerance — coordinator crashes after submitting, then the leader
/// crashes before deciding.  A new leader is elected and must recover the
/// pending entry via Phase 1.
///
/// N=7.  The scenario has two successive failures:
///
///   1. Coordinator C1 submits entry E1 and is killed after propagation.
///      The leader has E1 in its DOM buffer (or already accepted it) but has
///      not yet triggered the resend-timer slow path.
///
///   2. The leader is killed.  At least some followers have E1 as Undecided
///      in their logs.
///
///   3. A new leader is elected from the 5 remaining nodes.  Its Prepare phase
///      discovers E1 as the highest accepted-but-undecided entry and delivers
///      it via AcceptSync to any node that missed it.
///
///   4. A second entry E2 is submitted via a surviving coordinator after the
///      new leader reaches Phase::Accept.  All 5 nodes must eventually decide
///      both E1 and E2.
///
/// This tests the critical safety property: an entry accepted by a quorum
/// before a leader failure must not be lost.
#[test]
#[serial]
fn coordinator_and_leader_crash_new_leader_reconciles() {
    test_begin("coordinator_and_leader_crash_new_leader_reconciles");
    let cfg = dom_default_testcfg(None, None); // N=7
    let mut sys = TestGuard::new(
        TestSystem::with(cfg),
        "coordinator_and_leader_crash_new_leader_reconciles",
    );
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Wait for all nodes to reach Phase::Accept.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((_, true)))
            })
        });
    }

    // Pick a coordinator that is neither the leader.
    let c1 = (1..=cfg.num_nodes as NodeId)
        .find(|&p| p != leader)
        .expect("c1");
    let e1 = Value::with_id(4300);

    // Submit E1 then kill the coordinator.
    sys.nodes.get(&c1).unwrap().on_definition(|x| {
        x.paxos.append(e1.clone()).expect("append e1");
    });
    sleep(Duration::from_millis(60));
    sys.kill_node(c1);

    // Now kill the leader before the resend timer fires.
    // (Sleep a bit so E1 has propagated to most followers' DOM buffers.)
    sleep(Duration::from_millis(20));
    sys.kill_node(leader);

    // Wait for a new leader to be elected and reach Phase::Accept.
    let observer = *sys.nodes.keys().next().unwrap();
    wait_until(cfg.wait_timeout, || {
        sys.nodes.get(&observer).unwrap().on_definition(|x| {
            matches!(x.paxos.get_current_leader(), Some((ldr, _)) if ldr != leader)
        })
    });
    let l2 = sys.nodes.get(&observer).unwrap().on_definition(|x| {
        x.paxos.get_current_leader().map(|(ldr, _)| ldr).unwrap()
    });
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((ldr, true)) if ldr == l2)
            })
        });
    }

    // Submit a fresh entry E2 via any surviving non-leader coordinator.
    let c2 = sys
        .nodes
        .keys()
        .copied()
        .find(|&p| p != l2)
        .expect("c2");
    let e2 = Value::with_id(4301);
    let (kp2, kf2) = promise::<()>();
    sys.nodes.get(&c2).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kp2, e2.clone()));
        x.paxos.append(e2.clone()).expect("append e2");
    });
    kf2.wait_timeout(cfg.wait_timeout).expect("e2 not decided");

    // All 5 surviving nodes must have decided E1 (recovered) and E2.
    // E1 is always before E2 because E2 was submitted after the new leader
    // reached Accept phase, which happens after E1 was reconciled.
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 2
            })
        });
        let decided = read_decided_values(&sys, pid);
        assert_eq!(decided.len(), 2, "node {pid} should have 2 decided entries");
        assert!(decided.contains(&e1), "node {pid} is missing E1 (lost after dual failure)");
        assert!(decided.contains(&e2), "node {pid} is missing E2");
    }

    test_end("coordinator_and_leader_crash_new_leader_reconciles");
}

/// Fault tolerance — a minority partition is isolated during fast-path
/// activity, then the leader in the majority partition crashes.  After a new
/// leader is elected in the majority partition and the minority is reconnected,
/// the whole cluster must converge to the same log.
///
/// N=7.  Timeline:
///
///   1. Partition: nodes {isolated1, isolated2} are cut off from everyone else.
///      The 5-node majority can still decide (fast_quorum=6 is gone, but
///      majority quorum 4/7 is met by the 5 active nodes for slow path).
///
///   2. Two entries E1 and E2 are submitted by coordinators in the majority
///      partition.  The slow path is forced (only 5 nodes → < fast_quorum=6).
///      Both are decided on the 5 majority nodes.
///
///   3. The current leader (in the majority partition) is killed.
///      4 majority nodes remain.  A new leader is elected.
///
///   4. The minority partition is reconnected (2 isolated nodes rejoin).
///      A fresh entry E3 is submitted.  The new leader's AcceptSync brings
///      the isolated nodes' logs up to date.
///
///   5. All 6 surviving nodes decide [E1, E2, E3].
///
/// This exercises: partition tolerance + leader failure + log reconciliation
/// across a reconnected minority, all in a single test.
#[test]
#[serial]
fn partition_then_leader_crash_minority_rejoins() {
    test_begin("partition_then_leader_crash_minority_rejoins");
    let cfg = dom_default_testcfg(None, Some(Duration::from_millis(10_000))); // N=7
    let mut sys = TestGuard::new(
        TestSystem::with(cfg),
        "partition_then_leader_crash_minority_rejoins",
    );
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Wait for all nodes to reach Phase::Accept.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((_, true)))
            })
        });
    }

    // Isolate 2 non-leader nodes.
    let minority: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader)
        .take(2)
        .collect();
    let (iso1, iso2) = (minority[0], minority[1]);
    sys.set_node_connections(iso1, false);
    sys.set_node_connections(iso2, false);

    // Pick 2 coordinators from the 5-node majority (not the leader).
    let majority: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != leader && p != iso1 && p != iso2)
        .collect();
    let coord_a = majority[0];
    let coord_b = majority[1];

    let e1 = Value::with_id(4400);
    let e2 = Value::with_id(4401);

    // Submit E1 and E2 in the majority partition and wait for them to be
    // decided on all 5 majority nodes.  Fast quorum=6 cannot be met (only 5
    // nodes connected), so the slow path decides both entries.
    coordinator_append(&sys, coord_a, e1.clone());
    coordinator_append(&sys, coord_b, e2.clone());
    let majority_pids: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&p| p != iso1 && p != iso2)
        .collect();
    for pid in &majority_pids {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 2
            })
        });
    }

    // Kill the current leader within the majority.
    sys.kill_node(leader);

    // Wait for a new leader among the 4 remaining majority nodes.
    let observer = majority_pids
        .iter()
        .find(|&&p| p != leader)
        .copied()
        .expect("observer");
    wait_until(cfg.wait_timeout, || {
        sys.nodes.get(&observer).unwrap().on_definition(|x| {
            matches!(x.paxos.get_current_leader(), Some((ldr, _)) if ldr != leader)
        })
    });
    let l2 = sys.nodes.get(&observer).unwrap().on_definition(|x| {
        x.paxos.get_current_leader().map(|(ldr, _)| ldr).unwrap()
    });
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        if pid == iso1 || pid == iso2 {
            continue; // still isolated — can't reach Accept
        }
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((ldr, true)) if ldr == l2)
            })
        });
    }

    // Reconnect the minority and wait for them to finish Phase 1 recovery
    // (AcceptSync from l2) before submitting E3.  Without this wait, iso1/iso2
    // may still have accepted_idx=0 when E3 arrives, trigger the stale-log
    // guard, and initiate another Phase 1, delaying E3's decision and risking
    // the timeout when the full test suite runs back-to-back.
    sys.set_node_connections(iso1, true);
    sys.set_node_connections(iso2, true);
    for pid in [iso1, iso2] {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((ldr, true)) if ldr == l2)
            })
        });
    }

    // Submit a fresh entry E3 via a surviving coordinator.  All 6 nodes are
    // now in Phase::Accept under l2, so they all participate in the fast path.
    let c3 = sys
        .nodes
        .keys()
        .copied()
        .find(|&p| p != l2 && p != iso1 && p != iso2)
        .expect("c3");
    let e3 = Value::with_id(4402);
    let (kp3, kf3) = promise::<()>();
    sys.nodes.get(&c3).unwrap().on_definition(|x| {
        x.insert_decided_future(Ask::new(kp3, e3.clone()));
        x.paxos.append(e3.clone()).expect("append e3");
    });
    kf3.wait_timeout(cfg.wait_timeout).expect("e3 not decided");

    // All 6 surviving nodes must decide all 3 entries.
    // E1 and E2 were decided in the majority before E3 was submitted, so
    // their relative order is already fixed and consistent across the cluster.
    // E3 is always last.
    for pid in sys.nodes.keys().copied().collect::<Vec<_>>() {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                x.paxos.get_decided_idx() >= 3
            })
        });
        let decided = read_decided_values(&sys, pid);
        assert_eq!(decided.len(), 3, "node {pid} should have exactly 3 decided entries");
        assert!(decided.contains(&e1), "node {pid} missing E1");
        assert!(decided.contains(&e2), "node {pid} missing E2");
        assert_eq!(decided[2], e3, "node {pid}: E3 must be the last decided entry");
    }

    test_end("partition_then_leader_crash_minority_rejoins");
}

/// Stress test — 15 entries from 5 concurrent coordinators.
///
/// N=7, fast_quorum=6.  Five non-leader nodes each append 3 entries, with
/// proposals interleaved across coordinators in each round so that deadlines
/// collide and the leader is likely to rewrite some of them.  This exercises:
///
///   - Multiple concurrent DOM buffer entries competing for log slots
///   - Deadline rewrite path on the leader (late entries bumped forward)
///   - Hash-mismatch recovery when a follower accepted a pre-rewrite deadline
///   - Slow-path fallback (resend timer) for any entries that stall
///   - All DOM hashes converging to the same value on all 7 nodes
///
/// Because DOM deadline ordering is non-deterministic the test compares the
/// *set* of decided values rather than their exact sequence.
#[test]
#[serial]
fn many_entries_multiple_coordinators() {
    test_begin("many_entries_multiple_coordinators");
    let cfg = dom_default_testcfg(None, None); // 7 nodes, fast_quorum=6
    let sys = TestGuard::new(
        TestSystem::with(cfg),
        "many_entries_multiple_coordinators",
    );
    sys.start_all_nodes();

    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    // Wait for every node to reach Phase::Accept before proposing.
    // get_current_leader() returns Some((pid, is_accepted=true)) once the node
    // has received AcceptSync and transitioned out of Prepare.  Without this
    // wait, proposals submitted while nodes are still in Prepare phase fall
    // back to the slow path (ProposalForward → AcceptDecide), bypassing the
    // fast path entirely and leaving dom_hash=0 everywhere.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((_, true)))
            })
        });
    }

    // Pick 5 distinct non-leader coordinators.
    let coordinators: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&pid| pid != leader)
        .take(5)
        .collect();
    assert_eq!(coordinators.len(), 5, "need at least 5 non-leader nodes (N=7 guarantees this)");

    // 5 coordinators × 3 rounds = 15 entries.
    // Interleave by coordinator within each round so proposals from different
    // nodes are submitted back-to-back, maximising deadline collisions.
    const ROUNDS: u64 = 3;
    const COORDS: u64 = 5;
    let mut all_values: Vec<Value> = Vec::new();
    for round in 0..ROUNDS {
        for (i, &coord) in coordinators.iter().enumerate() {
            let id = round * COORDS + i as u64 + 1; // 1..=15, all unique
            let val = Value::with_id(id);
            all_values.push(val.clone());
            sys.nodes.get(&coord).unwrap().on_definition(|x| {
                x.paxos.append(val).expect("append should succeed");
            });
        }
    }
    assert_eq!(all_values.len(), 15);

    // All 7 nodes must decide all 15 entries.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(cfg.wait_timeout, || {
            sys.nodes
                .get(&pid)
                .unwrap()
                .on_definition(|x| x.paxos.get_decided_idx() >= 15)
        });
    }

    // Every node must decide the same *set* of values.
    // The decided order is deadline-determined (non-deterministic across runs)
    // so we check membership rather than exact sequence.
    for pid in 1..=cfg.num_nodes as NodeId {
        let decided = read_decided_values(&sys, pid);
        assert_eq!(
            decided.len(),
            15,
            "node {pid} decided {} entries, expected 15",
            decided.len()
        );
        for v in &all_values {
            assert!(
                decided.contains(v),
                "node {pid} is missing value {v:?} from its decided log"
            );
        }
    }

    // All 7 nodes must agree on the final DOM hash.
    // A diverged hash means at least one node applied different fast-path
    // metadata to its chain — a correctness bug.
    let leader_hash = get_dom_hash(&sys, leader);
    for pid in 1..=cfg.num_nodes as NodeId {
        let h = get_dom_hash(&sys, pid);
        assert_eq!(
            h, leader_hash,
            "node {pid} dom_hash={h:#018x} != leader dom_hash={leader_hash:#018x}"
        );
    }

    test_end("many_entries_multiple_coordinators");
}

/// Simulated traffic — 7 nodes, 4 coordinators, 120 requests in bursts
///
/// This test simulates realistic client traffic on a fully-connected 7-node
/// cluster.  Four non-leader nodes each act as coordinators and collectively
/// submit 120 requests (30 per coordinator).  Requests are sent in six bursts
/// of 20 proposals each (5 per coordinator per burst) with a short pause
/// between bursts, so consecutive proposals within a burst share very similar
/// deadlines — maximising the chance of concurrent fast-path racing.
///
/// All 7 nodes are connected throughout, so the fast quorum (N=7 → 6) is
/// always met and every entry should be decided via the DOM fast path.
///
/// The test waits for all 7 nodes to reach decided_idx >= TOTAL and relies on
/// the log output (parsed by log_parser/parse_log.py) for performance analysis.
#[test]
#[serial]
fn simulated_traffic() {
    test_begin("simulated_traffic");
    let cfg = dom_default_testcfg(None, None); // 7 nodes, fast_quorum=6
    let sys = TestGuard::new(TestSystem::with(cfg), "simulated_traffic");
    sys.start_all_nodes();

    let no_timeout = Duration::MAX;

    let leader = sys.get_elected_leader(1, no_timeout);

    // Wait for every node to enter Phase::Accept before proposing.
    // Without this, proposals arrive while nodes are still in Prepare and fall
    // back to the slow path, skewing the fast-path statistics we want to log.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(no_timeout, || {
            sys.nodes.get(&pid).unwrap().on_definition(|x| {
                matches!(x.paxos.get_current_leader(), Some((_, true)))
            })
        });
    }

    // Four distinct non-leader coordinators.
    let coordinators: Vec<NodeId> = (1..=cfg.num_nodes as NodeId)
        .filter(|&pid| pid != leader)
        .take(4)
        .collect();
    assert_eq!(coordinators.len(), 4, "need at least 4 non-leader nodes (N=7 guarantees this)");

    const NUM_COORDS: u64 = 4;
    const REQUESTS_PER_COORD: u64 = 4000;
    const BURSTS: u64 = 1000;
    const PER_COORD_PER_BURST: u64 = REQUESTS_PER_COORD / BURSTS;
    // Derive TOTAL from what is actually sent so integer division in
    // PER_COORD_PER_BURST never causes a mismatch when REQUESTS_PER_COORD
    // is not evenly divisible by BURSTS.
    const TOTAL: usize = (NUM_COORDS * BURSTS * PER_COORD_PER_BURST) as usize;

    for burst in 0..BURSTS {
        // Within each burst all 4 coordinators fire PER_COORD_PER_BURST
        // proposals back-to-back, making their deadlines nearly identical.
        for (c_idx, &coord) in coordinators.iter().enumerate() {
            let base = c_idx as u64 * REQUESTS_PER_COORD + burst * PER_COORD_PER_BURST;
            for i in 0..PER_COORD_PER_BURST {
                let id = base + i + 1; // unique across all coordinators and bursts
                sys.nodes.get(&coord).unwrap().on_definition(|x| {
                    x.paxos.append(Value::with_id(id)).expect("append should succeed");
                });
            }
        }
        // Inter-burst gap — ensures deadlines are well-separated across bursts.
        sleep(Duration::from_millis(1));
    }

    // Wait for all nodes to decide all entries.
    for pid in 1..=cfg.num_nodes as NodeId {
        wait_until(no_timeout, || {
            sys.nodes
                .get(&pid)
                .unwrap()
                .on_definition(|x| x.paxos.get_decided_idx() >= TOTAL)
        });
    }

    // All 7 nodes must converge on the same final DOM hash.
    let leader_hash = get_dom_hash(&sys, leader);
    for pid in 1..=cfg.num_nodes as NodeId {
        let h = get_dom_hash(&sys, pid);
        assert_eq!(
            h, leader_hash,
            "node {pid} dom_hash={h:#018x} != leader dom_hash={leader_hash:#018x}"
        );
    }

    // Grace period: allow the slog-async drain to flush all buffered log
    // messages before the Kompact system is dropped. Without this, the tail
    // of the log is truncated and the parser sees incomplete event chains.
    sleep(Duration::from_secs(2));

    test_end("simulated_traffic");
}
