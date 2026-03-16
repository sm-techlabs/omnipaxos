use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseMetaData},
};
use crate::dom::FastAcceptedOutcome;
use crate::util::{AcceptedMetaData, WRITE_ERROR_MSG};

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return;
        }
        #[cfg(feature = "logging")]
        debug!(self.logger, "Newly elected leader: {:?}", n);
        if self.pid == n.pid {
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[LEADER] elected ballot={:?} → entering Prepare phase, sending Prepare to {} peers",
                n,
                self.peers.len(),
            );
            self.leader_state =
                LeaderState::with(n, self.leader_state.max_pid, self.leader_state.quorum);
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage.set_promise(n).expect(WRITE_ERROR_MSG);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            #[cfg(feature = "logging")]
            self.update_state_label();
            let prep = Prepare {
                n,
                decided_idx,
                n_accepted: na,
                accepted_idx,
            };
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg: PaxosMsg::Prepare(prep),
                }));
            }
        } else {
            self.become_follower();
        }
    }

    pub(crate) fn become_follower(&mut self) {
        self.state.0 = Role::Follower;
        #[cfg(feature = "logging")]
        self.update_state_label();
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.leader_state.set_latest_accept_meta(from, None);
            self.send_prepare(from);
        }
    }

    pub(crate) fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        if !self.accepted_reconfiguration() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.buffered_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.accept_entries_leader(entries),
                _ => self.forward_proposals(entries),
            }
        }
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if self.accepted_reconfiguration() {
            return;
        }
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        }));
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[SEND][PREPARE] to={} ballot={:?} decided_idx={} accepted_idx={}",
            to,
            prep.n,
            prep.decided_idx,
            prep.accepted_idx,
        );
    }

    pub(crate) fn accept_entry_leader(&mut self, entry: T) {
        let accepted_metadata = self
            .internal_storage
            .append_entry_with_batching(entry)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn accept_entries_leader(&mut self, entries: Vec<T>) {
        let accepted_metadata = self
            .internal_storage
            .append_entries_with_batching(entries)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn accept_stopsign_leader(&mut self, ss: StopSign) {
        let accepted_metadata = self
            .internal_storage
            .append_stopsign(ss.clone())
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.send_acceptdecide(metadata);
        }
        let accepted_idx = self.internal_storage.get_accepted_idx();
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accept_stopsign(pid, ss.clone(), false);
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let current_n = self.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        // Follower can have valid accepted entries depending on which leader they were previously following
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        // Drain any DOM entries whose deadline has now passed so the suffix we
        // build below includes them.  Without this, a pending entry that was
        // accepted after the last tick() but before this AcceptSync would be
        // missing from the suffix; the recovering follower would enter Accept
        // with a stale accepted_idx and immediately need another recovery cycle.
        while let Some(prop_msg) = self.dom.release_message() {
            self.handle_released_fast_entry_leader(prop_msg);
        }
        let log_sync = self.create_log_sync(followers_valid_entries_idx, followers_decided_idx);
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.get_decided_idx(),
            log_sync,
            dom_hash: self.dom.last_log_hash,
            #[cfg(feature = "unicache")]
            unicache: self.internal_storage.get_unicache(),
        };
        #[cfg(feature = "logging")]
        let (log_decided_idx, log_seq_num) = (acc_sync.decided_idx, acc_sync.seq_num);
        let msg = Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        });
        self.outgoing.push(msg);
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[SEND][ACCEPT_SYNC] to={} ballot={:?} decided_idx={} seq_num={:?}",
            to,
            current_n,
            log_decided_idx,
            log_seq_num,
        );
    }

    fn send_acceptdecide(&mut self, accepted: AcceptedMetaData<T>) {
        let decided_idx = self.internal_storage.get_decided_idx();
        for pid in self.leader_state.get_promised_followers() {
            let latest_accdec = self.get_latest_accdec_message(pid);
            match latest_accdec {
                // Modify existing AcceptDecide message to follower
                Some(accdec) => {
                    accdec.entries.extend(accepted.entries.iter().cloned());
                    accdec.decided_idx = decided_idx;
                }
                // Add new AcceptDecide message to follower
                None => {
                    self.leader_state
                        .set_latest_accept_meta(pid, Some(self.outgoing.len()));
                    let acc = AcceptDecide {
                        n: self.leader_state.n_leader,
                        seq_num: self.leader_state.next_seq_num(pid),
                        decided_idx,
                        prev_idx: accepted.accepted_idx - accepted.entries.len(),
                        entries: accepted.entries.clone(),
                        id: (0, 0),
                        deadline: self.dom.get_deadline(),
                        dom_hash: self.dom.last_log_hash,
                    };
                    #[cfg(feature = "logging")]
                    let (acc_entries_len, acc_decided_idx, acc_seq_num) =
                        (acc.entries.len(), acc.decided_idx, acc.seq_num);
                    self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::AcceptDecide(acc),
                    }));
                    #[cfg(feature = "logging")]
                    info!(
                        self.logger,
                        "[SEND][ACCEPT_DECIDE] to={} entries={} decided_idx={} seq_num={:?}",
                        pid,
                        acc_entries_len,
                        acc_decided_idx,
                        acc_seq_num,
                    );
                }
            }
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.leader_state.n_leader,
            ss,
        });
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: acc_ss,
        }));
    }

    pub(crate) fn send_decide(&mut self, to: NodeId, decided_idx: usize, resend: bool, hash: u64) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let d = Decide {
            n: self.leader_state.n_leader,
            seq_num,
            decided_idx,
            hash,
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Decide(d),
        }));
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[SEND][DECIDE] to={} decided_idx={} hash={} resend={}",
            to,
            decided_idx,
            hash,
            resend,
        );
    }

    fn handle_majority_promises(&mut self) {
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let decided_idx = self.leader_state.get_max_decided_idx();
        let mut new_accepted_idx = self
            .internal_storage
            .sync_log(self.leader_state.n_leader, decided_idx, max_promise_sync)
            .expect(WRITE_ERROR_MSG);
        if !self.accepted_reconfiguration() {
            if !self.buffered_proposals.is_empty() {
                let entries = std::mem::take(&mut self.buffered_proposals);
                new_accepted_idx = self
                    .internal_storage
                    .append_entries_without_batching(entries)
                    .expect(WRITE_ERROR_MSG);
            }
            if let Some(ss) = self.buffered_stopsign.take() {
                self.internal_storage
                    .append_stopsign(ss)
                    .expect(WRITE_ERROR_MSG);
                new_accepted_idx = self.internal_storage.get_accepted_idx();
            }
        }
        // Slow-path entries (from sync_log and buffered_proposals) occupy log
        // positions 1..=new_accepted_idx without going through the DOM hash
        // chain.  Fill log_hashes with placeholders so that get_hash_at(pos)
        // returns the correct hash for subsequent fast-path entries at
        // positions > new_accepted_idx.
        self.dom.fill_to_log_position(new_accepted_idx);
        self.state = (Role::Leader, Phase::Accept);
        #[cfg(feature = "logging")]
        self.update_state_label();
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[LEADER] majority promises received → Accept phase, accepted_idx={} sending AcceptSync to {} followers",
            new_accepted_idx,
            self.leader_state.get_promised_followers().len(),
        );
        self.leader_state
            .set_accepted_idx(self.pid, new_accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            #[cfg(feature = "logging")]
            {
                let count = self.leader_state.accepted_indexes.iter()
                    .filter(|&&a| a >= accepted.accepted_idx)
                    .count();
                let quorum_size = self.leader_state.quorum.accept_quorum_size();
                info!(
                    self.logger,
                    "[RECV][ACCEPTED] from={} accepted_idx={} decided_idx={} quorum_progress={}/{}",
                    from,
                    accepted.accepted_idx,
                    self.internal_storage.get_decided_idx(),
                    count,
                    quorum_size,
                );
            }
            if accepted.accepted_idx > self.internal_storage.get_decided_idx()
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                #[cfg(feature = "logging")]
                info!(
                    self.logger,
                    "[LEADER][SLOW_PATH] quorum reached accepted_idx={} → deciding",
                    accepted.accepted_idx,
                );
                let decided_idx = accepted.accepted_idx;
                self.internal_storage
                    .set_decided_idx(decided_idx)
                    .expect(WRITE_ERROR_MSG);
                for pid in self.leader_state.get_promised_followers() {
                    let latest_accdec = self.get_latest_accdec_message(pid);
                    match latest_accdec {
                        Some(accdec) => accdec.decided_idx = decided_idx,
                        None => self.send_decide(pid, decided_idx, false, self.dom.last_log_hash),
                    }
                }
            }
        }
    }

    pub(crate) fn handle_fast_accepted(&mut self, accepted: FastAccepted, from: NodeId) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[RECV][FAST_ACCEPTED] from={} coordinator={} request={} accepted_idx={} hash={}",
            from,
            accepted.coordinator_id,
            accepted.request_id,
            accepted.accepted_idx,
            accepted.hash,
        );

        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > self.internal_storage.get_decided_idx() {
                match self.dom.handle_fast_accepted(accepted, from, self.pid) {
                    FastAcceptedOutcome::QuorumReached => {
                        #[cfg(feature = "logging")]
                        info!(
                            self.logger,
                            "[FAST_ACCEPTED][QUORUM] coordinator={} request={} accepted_idx={} \
                             fast_quorum_reached={}/{}",
                            accepted.coordinator_id,
                            accepted.request_id,
                            accepted.accepted_idx,
                            self.dom.fast_accepted_count(accepted.accepted_idx),
                            self.dom.fast_accepted_quorum_size(),
                        );
                        self.fast_decide(accepted.accepted_idx, accepted.hash);
                    }
                    FastAcceptedOutcome::HashMismatch => {
                        // Case 2: the leader already released this entry with a reordered
                        // deadline (and therefore a different hash).  The follower accepted
                        // the original deadline and has an inconsistent DOM hash.
                        // Send a Decide whose decided_idx equals accepted_idx so the
                        // follower knows exactly which position to patch in its hash chain.
                        if let Some(correct_hash) =
                            self.dom.get_hash_at(accepted.accepted_idx)
                        {
                            #[cfg(feature = "logging")]
                            info!(
                                self.logger,
                                "[FAST_ACCEPTED][HASH_MISMATCH] coordinator={} request={} \
                                 from={} accepted_idx={} follower_hash={} our_hash={} \
                                 → sending recovery Decide",
                                accepted.coordinator_id,
                                accepted.request_id,
                                from,
                                accepted.accepted_idx,
                                accepted.hash,
                                correct_hash,
                            );
                            self.send_decide(from, accepted.accepted_idx, false, correct_hash);
                        }
                        // If the leader hasn't released this slot yet (Case 1), the
                        // proactive Decide is sent from handle_released_fast_entry_leader
                        // once the entry is processed.
                    }
                    FastAcceptedOutcome::Pending(has_leader_fast_accepted) => {
                        #[cfg(feature = "logging")]
                        info!(
                            self.logger,
                            "[FAST_ACCEPTED][QUORUM] coordinator={} request={} accepted_idx={} \
                             pending={}/{} leader_fast_accepted={}",
                            accepted.coordinator_id,
                            accepted.request_id,
                            accepted.accepted_idx,
                            self.dom.fast_accepted_count(accepted.accepted_idx),
                            self.dom.fast_accepted_quorum_size(),
                            has_leader_fast_accepted,
                        );
                    }
                }
            }
        }
    }

    pub(crate) fn handle_released_fast_entry_leader(&mut self, prop_msg: AcceptDecide<T>) {
        let AcceptDecide {
            id,
            entries,
            ..
        } = prop_msg;
        let accepted_metadata = self
            .internal_storage
            .append_entries_with_batching(entries)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            let accepted_idx = metadata.accepted_idx;
            self.leader_state.set_accepted_idx(self.pid, accepted_idx);

            let hash = self
                .dom
                .record_accepted_metadata(accepted_idx);
            let fast_reply = FastReply {
                n: self.leader_state.n_leader,
                coordinator_id: id.0,
                request_id: id.1,
                replica_id: self.pid,
                accepted_idx: Some(accepted_idx),
                result: None,
                hash,
            };
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[INFO][FAST_PATH] leader appended coordinator={} request={} accepted_idx={} hash={}",
                id.0,
                id.1,
                accepted_idx,
                hash,
            );
            self.dispatch_fast_reply(fast_reply);

            let fast_accepted = FastAccepted {
                n: self.leader_state.n_leader,
                coordinator_id: id.0,
                request_id: id.1,
                accepted_idx,
                hash,
            };
            match self
                .dom
                .handle_fast_accepted(fast_accepted, self.pid, self.pid)
            {
                FastAcceptedOutcome::QuorumReached => {
                    if accepted_idx > self.internal_storage.get_decided_idx() {
                        self.fast_decide(accepted_idx, hash);
                    }
                }
                FastAcceptedOutcome::HashMismatch => {
                    // Case 1: some followers already sent FastAccepted with the original
                    // (pre-reorder) deadline before the leader processed this slot.
                    // Their hash H1 != our H2.  All nodes currently in the quorum tracker
                    // for this slot used H1 and need recovery.
                    // Use accepted_idx (not get_decided_idx()) as decided_idx so the
                    // follower knows exactly which position to patch in its hash chain.
                    let followers_to_recover: Vec<NodeId> = self
                        .dom
                        .get_replicas_with_wrong_hash(accepted_idx)
                        .into_iter()
                        .filter(|&pid| pid != self.pid)
                        .collect();
                    #[cfg(feature = "logging")]
                    if !followers_to_recover.is_empty() {
                        info!(
                            self.logger,
                            "[FAST_ACCEPTED][HASH_MISMATCH] coordinator={} request={} \
                             leader hash={} differs from follower hash at accepted_idx={}; \
                             triggering recovery on {:?}",
                            id.0,
                            id.1,
                            hash,
                            accepted_idx,
                            followers_to_recover,
                        );
                    }
                    for follower in followers_to_recover {
                        self.send_decide(follower, accepted_idx, false, hash);
                    }
                }
                FastAcceptedOutcome::Pending(_) => {}
            }
        }
    }

    fn fast_decide(&mut self, decided_idx: usize, hash: u64) {
        if decided_idx <= self.internal_storage.get_decided_idx() {
            return;
        }

        self.internal_storage
            .set_decided_idx(decided_idx)
            .expect(WRITE_ERROR_MSG);
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[INFO][FAST_DECIDE] decided_idx={} hash={} broadcasting Decide", decided_idx, hash,
        );
        let peers = self.peers.clone();
        for pid in peers {
            self.send_decide(pid, decided_idx, false, hash);
        }
    }

    fn get_latest_accdec_message(&mut self, to: NodeId) -> Option<&mut AcceptDecide<T>> {
        if let Some((bal, outgoing_idx)) = self.leader_state.get_latest_accept_meta(to) {
            if bal == self.leader_state.n_leader {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::AcceptDecide(accdec),
                    ..
                }) = self.outgoing.get_mut(outgoing_idx).unwrap()
                {
                    return Some(accdec);
                } else {
                    #[cfg(feature = "logging")]
                    debug!(self.logger, "Cached idx is not an AcceptedDecide!");
                }
            }
        }
        None
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        if self.state.0 == Role::Leader && self.leader_state.n_leader < not_acc.n {
            self.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Accept => {
                // Resend AcceptStopSign or StopSign's decide
                if let Some(ss) = self.internal_storage.get_stopsign() {
                    let decided_idx = self.internal_storage.get_decided_idx();
                    for follower in self.leader_state.get_promised_followers() {
                        if self.internal_storage.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true, self.dom.last_log_hash);
                        } else if self.leader_state.get_accepted_idx(follower)
                            != self.internal_storage.get_accepted_idx()
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                // Resend Prepare to any followers that haven't yet promised this term.
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
                // Fast-path stall recovery: if there are log entries accepted by the leader
                // but not yet decided (fast path quorum was not met), fall back to the slow path.
                // By the time this resend fires, followers that received the FastPropose and
                // processed it via the DOM buffer will have sent FastAccepted and their
                // leader_state accepted_idx will be current. Only followers that are still
                // missing entries (network issue or delayed DOM release) receive AcceptDecide.
                let decided_idx = self.internal_storage.get_decided_idx();
                let accepted_idx = self.internal_storage.get_accepted_idx();
                if accepted_idx > decided_idx {
                    if self.leader_state.is_chosen(accepted_idx) {
                        // Quorum already acknowledged the entry but fast_decide was not called
                        // (e.g., DOM quorum tracking lagged). Decide now via slow path.
                        #[cfg(feature = "logging")]
                        info!(
                            self.logger,
                            "[SLOW_PATH][DECIDE] quorum met on resend; deciding accepted_idx={}",
                            accepted_idx,
                        );
                        self.internal_storage
                            .set_decided_idx(accepted_idx)
                            .expect(WRITE_ERROR_MSG);
                        let peers = self.peers.clone();
                        for pid in peers {
                            self.send_decide(pid, accepted_idx, false, self.dom.last_log_hash);
                        }
                    } else {
                        // Quorum not yet met. Send AcceptDecide to every promised follower
                        // that is still behind, so they can enter the slow path.
                        #[cfg(feature = "logging")]
                        info!(
                            self.logger,
                            "[SLOW_PATH] fast-path stall; decided_idx={} accepted_idx={}; \
                             sending AcceptDecide to lagging followers",
                            decided_idx,
                            accepted_idx,
                        );
                    }
                    // In both cases, push missing log entries to any promised follower
                    // that has not yet acknowledged them.  This ensures followers that
                    // missed fast-path entries receive them and that the leader's
                    // per-follower seq_num counter is advanced, so any follower that is
                    // still unreachable will see a gap in the next AcceptDecide it
                    // receives and trigger Phase 1 recovery.
                    let lagging: Vec<NodeId> = self
                        .leader_state
                        .get_promised_followers()
                        .into_iter()
                        .filter(|pid| {
                            self.leader_state.get_accepted_idx(*pid) < accepted_idx
                        })
                        .collect();
                    for pid in lagging {
                        self.send_fallback_acceptdecide(pid);
                    }
                }
            }
            Phase::Recover => (),
            Phase::None => (),
        }
    }

    /// Sends an AcceptDecide to `to` covering all log entries from the follower's last known
    /// accepted index up to the leader's current accepted index.  Used as a slow-path fallback
    /// when a fast-path entry has not been acknowledged by this follower.
    #[cfg(not(feature = "unicache"))]
    fn send_fallback_acceptdecide(&mut self, to: NodeId) {
        let follower_accepted = self.leader_state.get_accepted_idx(to);
        let leader_accepted = self.internal_storage.get_accepted_idx();
        if follower_accepted >= leader_accepted {
            return;
        }
        match self.internal_storage.get_suffix(follower_accepted) {
            Ok(entries) if !entries.is_empty() => {
                let decided_idx = self.internal_storage.get_decided_idx();
                #[cfg(feature = "logging")]
                info!(
                    self.logger,
                    "[SEND][SLOW_PATH] AcceptDecide fallback to={} \
                     from_idx={} entries={} decided_idx={}",
                    to,
                    follower_accepted,
                    entries.len(),
                    decided_idx,
                );
                let acc = AcceptDecide {
                    n: self.leader_state.n_leader,
                    seq_num: self.leader_state.next_seq_num(to),
                    decided_idx,
                    prev_idx: follower_accepted,
                    entries,
                    id: (0, 0),
                    deadline: self.dom.get_deadline(),
                    dom_hash: self.dom.last_log_hash,
                };
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to,
                    msg: PaxosMsg::AcceptDecide(acc),
                }));
            }
            _ => {}
        }
    }

    pub(crate) fn flush_batch_leader(&mut self) {
        let accepted_metadata = self
            .internal_storage
            .flush_batch_and_get_entries()
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }
}
