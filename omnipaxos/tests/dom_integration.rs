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
