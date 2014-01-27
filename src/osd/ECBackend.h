// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECBACKEND_H
#define ECBACKEND_H

#include "OSD.h"
#include "PGBackend.h"
#include "osd_types.h"
#include <boost/optional.hpp>
#include "ErasureCodeInterface.h"
#include "ECTransaction.h"
#include "ECMsgTypes.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"

struct RecoveryMessages;
class ECBackend : public PGBackend {
public:
  RecoveryHandle *open_recovery_op();

  void run_recovery_op(
    RecoveryHandle *h,
    int priority
    );

  void recover_object(
    const hobject_t &hoid,
    eversion_t v,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h
    );

  bool handle_message(
    OpRequestRef op
    );
  bool can_handle_while_inactive(
    OpRequestRef op
    );
  friend struct SubWriteApplied;
  friend struct SubWriteCommitted;
  void sub_write_applied(
    tid_t tid, eversion_t version);
  void sub_write_committed(
    tid_t tid, eversion_t version);
  void handle_sub_write(
    pg_shard_t from,
    OpRequestRef msg,
    ECSubWrite &op,
    Context *on_local_applied_sync = 0
    );
  void handle_sub_read(
    pg_shard_t from,
    ECSubRead &op,
    ECSubReadReply *reply
    );
  void handle_sub_write_reply(
    pg_shard_t from,
    ECSubWriteReply &op
    );
  void handle_sub_read_reply(
    pg_shard_t from,
    ECSubReadReply &op
    );

  void check_recovery_sources(const OSDMapRef osdmap);

  void _on_change(ObjectStore::Transaction *t);
  void clear_state();

  void on_flushed();

  void dump_recovery_info(Formatter *f) const;

  PGTransaction *get_transaction();

  void submit_transaction(
    const hobject_t &hoid,
    const eversion_t &at_version,
    PGTransaction *t,
    const eversion_t &trim_to,
    vector<pg_log_entry_t> &log_entries,
    Context *on_local_applied_sync,
    Context *on_all_applied,
    Context *on_all_commit,
    tid_t tid,
    osd_reqid_t reqid,
    OpRequestRef op
    );

  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist *bl);

  void objects_read_async(
    const hobject_t &hoid,
    const list<pair<pair<uint64_t, uint64_t>,
		    pair<bufferlist*, Context*> > > &to_read,
    Context *on_complete);

private:
  friend struct ECRecoveryHandle;
  uint64_t get_recovery_chunk_size() const {
    uint64_t max = cct->_conf->osd_recovery_max_chunk;
    max -= max % stripe_width;
    max += stripe_width;
    return max;
  }
  struct RecoveryOp {
    hobject_t hoid;
    eversion_t v;
    set<pg_shard_t> missing_on;
    set<shard_id_t> missing_on_shards;

    ObjectRecoveryInfo recovery_info;
    ObjectRecoveryProgress recovery_progress;

    bool pending_read;
    enum { IDLE, READING, WRITING, COMPLETE } state;

    // must be filled if state == WRITING
    map<shard_id_t, bufferlist> returned_data;
    map<string, bufferlist> xattrs;
    ObjectContextRef obc;
    set<pg_shard_t> waiting_on_pushes;

    // valid in state READING
    pair<uint64_t, uint64_t> extent_requested;

    RecoveryOp() : pending_read(false), state(IDLE) {}
  };
  friend ostream &operator<<(ostream &lhs, const RecoveryOp &rhs);
  map<hobject_t, RecoveryOp> recovery_ops;
  struct ReadOp {
    OpRequestRef op; // may be null if not on behalf of a client
    tid_t tid;
    list<
      pair<
	hobject_t,
	boost::tuple<
	  uint64_t, uint64_t, bufferlist*, map<shard_id_t, bufferlist*> >
	>
      > to_read;
    map<
      pg_shard_t,
      list<
	pair<
	  hobject_t,
	  pair<uint64_t, bufferlist>
	  >
	>
      > complete;
    map<hobject_t, map<string, bufferlist> *> attrs_to_read;
    set<pg_shard_t> in_progress;
    Context *on_complete;

    ReadOp() : on_complete(NULL) {}
    ~ReadOp() { delete on_complete; }
  };
  friend ostream &operator<<(ostream &lhs, const ReadOp &rhs);
  map<tid_t, ReadOp> tid_to_read_map;
  map<pg_shard_t, set<tid_t> > shard_to_read_map;
  void start_read_op(
    const list<
      pair<
	hobject_t,
	boost::tuple<
	  uint64_t, uint64_t, bufferlist*, map<shard_id_t, bufferlist*> >
	>
      > &to_read,
    const map<hobject_t, map<string, bufferlist> *> &attrs_to_read,
    Context *c,
    OpRequestRef op,
    bool for_recovery = false);
  void cancel_read_op(
    tid_t tid);
  void restart_read_op(
    ReadOp &op);

  struct Op {
    hobject_t hoid;
    eversion_t version;
    eversion_t trim_to;
    vector<pg_log_entry_t> log_entries;
    Context *on_local_applied_sync;
    Context *on_all_applied;
    Context *on_all_commit;
    tid_t tid;
    osd_reqid_t reqid;
    OpRequestRef client_op;

    ECTransaction *t;

    set<hobject_t> temp_added;
    set<hobject_t> temp_cleared;

    set<pg_shard_t> pending_commit;
    set<pg_shard_t> pending_apply;
    ~Op() {
      delete t;
      delete on_local_applied_sync;
      delete on_all_applied;
      delete on_all_commit;
    }
  };
  friend ostream &operator<<(ostream &lhs, const Op &rhs);

  void continue_recovery_op(
    RecoveryOp &op,
    RecoveryMessages *m);

  void dispatch_recovery_messages(RecoveryMessages &m);
  friend struct OnRecoveryReadComplete;
  void handle_recovery_read_complete(
    const hobject_t &hoid,
    list<
      boost::tuple<uint64_t, uint64_t, map<shard_id_t, bufferlist> > > &to_read,
    map<string, bufferlist> *attrs,
    RecoveryMessages *m);
  void handle_recovery_push(
    PushOp &op,
    RecoveryMessages *m);
  void handle_recovery_push_reply(
    PushReplyOp &op,
    pg_shard_t from,
    RecoveryMessages *m);

  set<hobject_t> unstable;

  map<tid_t, Op> tid_to_op_map; /// lists below point into here
  list<Op*> writing;

  CephContext *cct;
  ErasureCodeInterfaceRef ec_impl;

  class ECDec : public PeeringContinueDecider {
    set<int> want;
    ErasureCodeInterfaceRef ec_impl;
  public:
    ECDec(ErasureCodeInterfaceRef ec_impl) : ec_impl(ec_impl) {
      for (unsigned i = 0; i < ec_impl->get_data_chunk_count(); ++i) {
	want.insert(i);
      }
    }
    bool operator()(const set<int> &have) const {
      set<int> min;
      return ec_impl->minimum_to_decode(want, have, &min) == 0;
    }
  };
  PeeringContinueDecider *get_peering_continue_decider() {
    return new ECDec(ec_impl);
  }

  class ECReadPred : public IsReadablePredicate {
    set<int> want;
    ErasureCodeInterfaceRef ec_impl;
  public:
    ECReadPred(ErasureCodeInterfaceRef ec_impl) : ec_impl(ec_impl) {
      for (unsigned i = 0; i < ec_impl->get_data_chunk_count(); ++i) {
	want.insert(i);
      }
    }
    bool operator()(const set<pg_shard_t> &_have) const {
      set<int> have;
      for (set<pg_shard_t>::const_iterator i = _have.begin();
	   i != _have.end();
	   ++i) {
	have.insert(i->shard);
      }
      set<int> min;
      return ec_impl->minimum_to_decode(want, have, &min) == 0;
    }
  };
  IsReadablePredicate *get_is_readable_predicate() {
    return new ECReadPred(ec_impl);
  }


  const uint64_t stripe_width;
  const uint64_t stripe_size;

  friend struct ReadCB;
  void check_op(Op *op);
  void start_write(Op *op);
public:
  ECBackend(
    PGBackend::Listener *pg,
    coll_t coll,
    coll_t temp_coll,
    ObjectStore *store,
    CephContext *cct,
    ErasureCodeInterfaceRef ec_impl);

  int get_min_avail_to_read(
    const hobject_t &hoid,
    set<pg_shard_t> *to_read);

  int get_min_avail_to_read_shards(
    const hobject_t &hoid,
    const set<int> &want,
    set<pg_shard_t> *to_read);

  void rollback_append(
    const hobject_t &hoid,
    uint64_t old_size,
    ObjectStore::Transaction *t);

  struct ECContext {
    list<pair<pg_shard_t, Message*> > to_send;
    list<Context*> to_run;
  };
};

#endif
