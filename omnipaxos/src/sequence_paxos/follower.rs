use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::util::{MessageStatus, WRITE_ERROR_MSG};

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** Follower ***/
    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) {
        let old_promise = self.internal_storage.get_promise();
        if old_promise < prep.n || (old_promise == prep.n && self.state.1 == Phase::Recover) {
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[RECV][PREPARE] from={} ballot={:?} their_decided_idx={} their_accepted_idx={} → sending Promise",
                from,
                prep.n,
                prep.decided_idx,
                prep.accepted_idx,
            );
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_promise(prep.n)
                .expect(WRITE_ERROR_MSG);
            self.state = (Role::Follower, Phase::Prepare);
            #[cfg(feature = "logging")]
            self.update_state_label();
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let log_sync = if na > prep.n_accepted {
                // I'm more up to date: send leader what he is missing after his decided index.
                Some(self.create_log_sync(prep.decided_idx, prep.decided_idx))
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // I'm more up to date and in same round: send leader what he is missing after his
                // accepted index.
                Some(self.create_log_sync(prep.accepted_idx, prep.decided_idx))
            } else {
                // I'm equally or less up to date
                None
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_idx: self.internal_storage.get_decided_idx(),
                accepted_idx,
                log_sync,
            };
            self.cached_promise_message = Some(promise.clone());
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            }));
        }
    }

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            self.cached_promise_message = None;
            let new_accepted_idx = self
                .internal_storage
                .sync_log(accsync.n, accsync.decided_idx, Some(accsync.log_sync))
                .expect(WRITE_ERROR_MSG);
            if self.internal_storage.get_stopsign().is_none() {
                self.forward_buffered_proposals();
            }
            let accepted = Accepted {
                n: accsync.n,
                accepted_idx: new_accepted_idx,
            };
            self.state = (Role::Follower, Phase::Accept);
            #[cfg(feature = "logging")]
            self.update_state_label();
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[RECV][ACCEPT_SYNC] from={} ballot={:?} decided_idx={} new_accepted_idx={} seq_num={:?} → entering Accept phase",
                from,
                accsync.n,
                accsync.decided_idx,
                new_accepted_idx,
                accsync.seq_num,
            );
            self.current_seq_num = accsync.seq_num;
            // Re-anchor the DOM hash chain to the leader's state and fill
            // log_hashes to new_accepted_idx so that get_hash_at(pos) is
            // correct for all subsequent fast-path entries.
            self.dom.sync_to_log_position(accsync.dom_hash, new_accepted_idx);
            let cached_idx = self.outgoing.len();
            self.latest_accepted_meta = Some((accsync.n, cached_idx));
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            }));
            #[cfg(feature = "unicache")]
            self.internal_storage.set_unicache(accsync.unicache);
        }
    }

    fn forward_buffered_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.buffered_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) fn handle_acceptdecide(
        &mut self,
        mut acc_dec: AcceptDecide<T>,
        is_from_early_buffer: bool,
    ) {
        let expected_sequence = if is_from_early_buffer {
            true
        } else {
            self.handle_sequence_num(acc_dec.seq_num, acc_dec.n.pid) == MessageStatus::Expected
        };

        if (is_from_early_buffer || self.check_valid_ballot(acc_dec.n))
            && self.state == (Role::Follower, Phase::Accept)
            && expected_sequence
        {
            // Capture fields before entries are moved
            let n = acc_dec.n;
            let decided_idx = acc_dec.decided_idx;

            // Fast-path entries bypass the seq_num ordering check, so we must
            // explicitly guard against accepting an entry at the wrong log
            // position.  If our accepted_idx is behind the decided_idx carried
            // in the message, we have missed entries that were already decided
            // and must recover before appending anything.
            if is_from_early_buffer
                && self.internal_storage.get_accepted_idx() < decided_idx
            {
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "[FAST_PATH] stale log: accepted_idx={} < decided_idx={} coordinator={} \
                     → discarding fast-path entry and triggering recovery",
                    self.internal_storage.get_accepted_idx(),
                    decided_idx,
                    n.pid,
                );
                self.reconnected(n.pid);
                return;
            }
            // Idempotency guard for slow-path (AcceptDecide) messages.
            // If this follower has already appended some or all of the entries in this
            // message (e.g., via DOM early_buffer release followed by a fallback
            // AcceptDecide), skip or trim the prefix we already have.
            if !is_from_early_buffer {
                let my_idx = self.internal_storage.get_accepted_idx();
                let entries_end = acc_dec.prev_idx + acc_dec.entries.len();
                if my_idx >= entries_end {
                    // All entries already in the log — just advance decided_idx.
                    #[cfg(feature = "logging")]
                    info!(
                        self.logger,
                        "[ACCEPT_DECIDE][IDEMPOTENT] skipping duplicate: my_idx={} entries=[{}..{}]",
                        my_idx,
                        acc_dec.prev_idx,
                        entries_end,
                    );
                    self.update_decided_idx_and_get_accepted_idx(acc_dec.decided_idx);
                    return;
                }
                if my_idx > acc_dec.prev_idx {
                    // Trim the already-appended prefix.
                    let skip = my_idx - acc_dec.prev_idx;
                    #[cfg(feature = "logging")]
                    info!(
                        self.logger,
                        "[ACCEPT_DECIDE][IDEMPOTENT] trimming {} already-appended entries: my_idx={} prev_idx={}",
                        skip,
                        my_idx,
                        acc_dec.prev_idx,
                    );
                    acc_dec.entries.drain(..skip);
                }
            }

            let deadline = acc_dec.deadline;
            let id = acc_dec.id;
            let dom_hash = acc_dec.dom_hash;

            #[cfg(not(feature = "unicache"))]
            let entries = acc_dec.entries;
            #[cfg(feature = "unicache")]
            let entries = self.internal_storage.decode_entries(acc_dec.entries);
            let mut new_accepted_idx = self
                .internal_storage
                .append_entries_and_get_accepted_idx(entries)
                .expect(WRITE_ERROR_MSG);
            let flushed_after_decide = self.update_decided_idx_and_get_accepted_idx(decided_idx);
            if flushed_after_decide.is_some() {
                new_accepted_idx = flushed_after_decide;
            }
            if let Some(idx) = new_accepted_idx {
                #[cfg(feature = "logging")]
                info!(
                    self.logger,
                    "[RECV][ACCEPT_DECIDE] accepted_idx={} decided_idx={} fast_path={}",
                    idx,
                    decided_idx,
                    is_from_early_buffer,
                );
                if is_from_early_buffer {
                    self.reply_fast_accepted(idx, deadline, id);
                } else {
                    // Slow-path: adopt the leader's DOM hash so our hash stays
                    // consistent with nodes that accepted the same entries via fast path.
                    self.dom.last_log_hash = dom_hash;
                    self.reply_accepted(n, idx);
                }
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.check_valid_ballot(acc_ss.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            // Flush entries before appending stopsign. The accepted index is ignored here as
            // it will be updated when appending stopsign.
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            let new_accepted_idx = self
                .internal_storage
                .set_stopsign(Some(acc_ss.ss))
                .expect(WRITE_ERROR_MSG);
            self.reply_accepted(acc_ss.n, new_accepted_idx);
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            // If the follower is missing entries that have already been decided, it
            // cannot safely advance its decided_idx — trigger Phase 1 recovery so
            // AcceptSync delivers the missing log suffix.
            if dec.decided_idx > self.internal_storage.get_accepted_idx() {
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "[DECIDE] missing entries: accepted_idx={} < decided_idx={}. \
                     Initiating Phase 1 recovery.",
                    self.internal_storage.get_accepted_idx(),
                    dec.decided_idx,
                );
                self.reconnected(dec.n.pid);
                return;
            }

            // Fast-path DOM hash correction: compare the leader's hash for
            // decided_idx against the follower's hash *at that same position*,
            // not against last_log_hash.  The follower may have fast-accepted
            // entries beyond decided_idx whose hashes are still valid; blindly
            // overwriting last_log_hash would corrupt them.
            if dec.hash != 0 {
                let our_hash_at_decided = self
                    .dom
                    .get_hash_at(dec.decided_idx)
                    .unwrap_or(self.dom.last_log_hash);
                if dec.hash != our_hash_at_decided {
                    #[cfg(feature = "logging")]
                    info!(
                        self.logger,
                        "[DECIDE][DOM_HASH] patching hash chain at decided_idx={}: \
                         {} → {} (deadline reorder correction)",
                        dec.decided_idx,
                        our_hash_at_decided,
                        dec.hash,
                    );
                    self.dom.patch_hash_at(dec.decided_idx, dec.hash);
                }
            }

            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[RECV][DECIDE] from={} decided_idx={} hash={} → committed",
                dec.n.pid,
                dec.decided_idx,
                dec.hash,
            );
            let new_accepted_idx = self.update_decided_idx_and_get_accepted_idx(dec.decided_idx);
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(dec.n, idx);
            }
        }
    }

    /// To maintain decided index <= accepted index, batched entries may be flushed.
    /// Returns `Some(new_accepted_idx)` if entries are flushed, otherwise `None`.
    fn update_decided_idx_and_get_accepted_idx(&mut self, new_decided_idx: usize) -> Option<usize> {
        if new_decided_idx <= self.internal_storage.get_decided_idx() {
            return None;
        }
        if new_decided_idx > self.internal_storage.get_accepted_idx() {
            let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_decided_idx(new_decided_idx.min(new_accepted_idx))
                .expect(WRITE_ERROR_MSG);
            Some(new_accepted_idx)
        } else {
            self.internal_storage
                .set_decided_idx(new_decided_idx)
                .expect(WRITE_ERROR_MSG);
            None
        }
    }

    fn reply_accepted(&mut self, n: Ballot, accepted_idx: usize) {
        let latest_accepted = self.get_latest_accepted_message(n);
        match latest_accepted {
            Some(acc) => acc.accepted_idx = accepted_idx,
            None => {
                let accepted = Accepted { n, accepted_idx };
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((n, cached_idx));
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: n.pid,
                    msg: PaxosMsg::Accepted(accepted),
                }));
                #[cfg(feature = "logging")]
                info!(
                    self.logger,
                    "[SEND][ACCEPTED] to={} accepted_idx={}",
                    n.pid,
                    accepted_idx,
                );
            }
        }
    }

    pub(crate) fn handle_released_fast_entry_follower(
        &mut self,
        prop_msg: AcceptDecide<T>,
    ) -> FastReply<T> {
        let coordinator_id = prop_msg.id.0;
        let request_id = prop_msg.id.1;
        self.handle_acceptdecide(prop_msg, true);
        let accepted_idx = self.internal_storage.get_accepted_idx();
        let hash = self
            .dom
            .get_hash_at(accepted_idx)
            .unwrap_or(self.dom.last_log_hash);

        FastReply {
            n: self.get_promise(),
            coordinator_id,
            request_id,
            replica_id: self.pid,
            accepted_idx: Some(accepted_idx),
            result: None,
            hash,
        }
    }

    fn reply_fast_accepted(&mut self, accepted_idx: usize, _deadline: i64, id: (u64, u64)) {
        let hash = self
            .dom
            .record_accepted_metadata(accepted_idx);
        let fast_accepted = FastAccepted {
            n: self.get_promise(),
            coordinator_id: id.0,
            request_id: id.1,
            accepted_idx,
            hash,
        };
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[SEND][FAST_ACCEPTED] to={} request={} accepted_idx={} hash={}",
            self.get_current_leader(),
            id.1,
            accepted_idx,
            hash,
        );
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to: self.get_current_leader(),
            msg: PaxosMsg::FastAccepted(fast_accepted),
        }));
    }

    fn get_latest_accepted_message(&mut self, n: Ballot) -> Option<&mut Accepted> {
        if let Some((ballot, outgoing_idx)) = &self.latest_accepted_meta {
            if *ballot == n {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::Accepted(a),
                    ..
                }) = self.outgoing.get_mut(*outgoing_idx).unwrap()
                {
                    return Some(a);
                } else {
                    #[cfg(feature = "logging")]
                    debug!(self.logger, "Cached idx is not an Accepted message!");
                }
            }
        }
        None
    }

    /// Also returns whether the message's ballot was promised
    fn check_valid_ballot(&mut self, message_ballot: Ballot) -> bool {
        let my_promise = self.internal_storage.get_promise();
        match my_promise.cmp(&message_ballot) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Greater => {
                let not_acc = NotAccepted { n: my_promise };
                #[cfg(feature = "logging")]
                trace!(
                    self.logger,
                    "NotAccepted. My promise: {:?}, theirs: {:?}",
                    my_promise,
                    message_ballot
                );
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: message_ballot.pid,
                    msg: PaxosMsg::NotAccepted(not_acc),
                }));
                false
            }
            std::cmp::Ordering::Less => {
                // Should never happen, but to be safe send PrepareReq
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "Received non-prepare message from a leader I've never promised. My: {:?}, theirs: {:?}", my_promise, message_ballot
                );
                self.reconnected(message_ballot.pid);
                false
            }
        }
    }

    /// Also returns the MessageStatus of the sequence based on the incoming sequence number.
    fn handle_sequence_num(&mut self, seq_num: SequenceNumber, from: NodeId) -> MessageStatus {
        let msg_status = self.current_seq_num.check_msg_status(seq_num);
        match msg_status {
            MessageStatus::Expected => self.current_seq_num = seq_num,
            MessageStatus::DroppedPreceding => {
                #[cfg(feature = "logging")]
                info!(
                    self.logger,
                    "[SEQ_GAP] expected={} got={:?} from={} → triggering recovery",
                    self.current_seq_num.counter + 1,
                    seq_num,
                    from,
                );
                self.reconnected(from);
            }
            MessageStatus::Outdated => (),
        };
        msg_status
    }

    pub(crate) fn resend_messages_follower(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Promise
                match &self.cached_promise_message {
                    Some(promise) => {
                        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                            from: self.pid,
                            to: promise.n.pid,
                            msg: PaxosMsg::Promise(promise.clone()),
                        }));
                    }
                    None => {
                        // Shouldn't be possible to be in prepare phase without having
                        // cached the promise sent as a response to the prepare
                        #[cfg(feature = "logging")]
                        warn!(self.logger, "In Prepare phase without a cached promise!");
                        self.state = (Role::Follower, Phase::Recover);
                        #[cfg(feature = "logging")]
                        self.update_state_label();
                        self.send_preparereq_to_all_peers();
                    }
                }
            }
            Phase::Recover => {
                // Resend PrepareReq
                self.send_preparereq_to_all_peers();
            }
            Phase::Accept => (),
            Phase::None => (),
        }
    }

    fn send_preparereq_to_all_peers(&mut self) {
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        for peer in &self.peers {
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: *peer,
                msg: PaxosMsg::PrepareReq(prepreq),
            }));
        }
    }

    pub(crate) fn flush_batch_follower(&mut self) {
        let accepted_idx = self.internal_storage.get_accepted_idx();
        let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
        if new_accepted_idx > accepted_idx {
            self.reply_accepted(self.get_promise(), new_accepted_idx);
        }
    }
}
