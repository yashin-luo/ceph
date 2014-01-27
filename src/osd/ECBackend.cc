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

#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <iostream>
#include <sstream>

#include "ECUtil.h"
#include "ECBackend.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ECBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

struct ECRecoveryHandle : public PGBackend::RecoveryHandle {
  list<ECBackend::RecoveryOp> ops;
};

ostream &operator<<(ostream &lhs, const ECBackend::ReadOp &rhs)
{
  lhs << "ReadOp(to_read=[";
  for (list<
	 pair<
	   hobject_t,
	   boost::tuple<
	     uint64_t, uint64_t, bufferlist*, map<shard_id_t, bufferlist*> >
	   >
	 >::const_iterator i = rhs.to_read.begin();
       i != rhs.to_read.end();
       ++i) {
    if (i != rhs.to_read.begin())
      lhs << ", ";
    lhs << i->first << "->"
	<< "(" << i->second.get<0>()
	<< ", " << i->second.get<1>()
	<< ", " << i->second.get<2>()
	<< ", " << i->second.get<3>()
	<< ")";
  }
  lhs << "] complete=";
  for (map<
	 pg_shard_t,
	 list<
	   pair<
	     hobject_t,
	     pair<uint64_t, bufferlist>
	     >
	   >
	 >::const_iterator i = rhs.complete.begin();
       i != rhs.complete.end();
       ++i) {
    if (i != rhs.complete.begin())
      lhs << ", ";
    lhs << i->first << ": {";
    for (list<
	   pair<
	     hobject_t,
	     pair<uint64_t, bufferlist>
	     >
	   >::const_iterator j = i->second.begin();
	 j != i->second.end();
	 ++j) {
      if (j != i->second.begin())
	lhs << ", ";
      lhs << j->first << "->"
	  << make_pair(j->second.first, j->second.second.length());
    }
    lhs << "}";
  }
  return lhs << "] attrs_to_read=" << rhs.attrs_to_read
	     << " in_progress=" << rhs.in_progress
	     << " tid=" << rhs.tid << ")";
}

ostream &operator<<(ostream &lhs, const ECBackend::Op &rhs)
{
  lhs << "Op(" << rhs.hoid
      << " v=" << rhs.version
      << " tt=" << rhs.trim_to
      << " tid=" << rhs.tid
      << " reqid=" << rhs.reqid;
  if (rhs.client_op && rhs.client_op->get_req()) {
    lhs << " client_op=";
    rhs.client_op->get_req()->print(lhs);
  }
  lhs << " pending_commit=" << rhs.pending_commit
      << " pending_apply=" << rhs.pending_apply
      << ")";
  return lhs;
}

ostream &operator<<(ostream &lhs, const ECBackend::RecoveryOp &rhs)
{
  lhs << "RecoveryOp("
      << "hoid=" << rhs.hoid
      << " v=" << rhs.v
      << " missing_on=" << rhs.missing_on
      << " missing_on_shards=" << rhs.missing_on_shards
      << " recovery_info=" << rhs.recovery_info
      << " recovery_progress=" << rhs.recovery_progress
      << " pending_read=" << rhs.pending_read
      << " obc refcount=" << rhs.obc.use_count()
      << " state=";
  switch (rhs.state) {
  case ECBackend::RecoveryOp::IDLE:
    lhs << "IDLE";
    break;
  case ECBackend::RecoveryOp::READING:
    lhs << "READING";
    break;
  case ECBackend::RecoveryOp::WRITING:
    lhs << "WRITING";
    break;
  case ECBackend::RecoveryOp::COMPLETE:
    lhs << "COMPLETE";
    break;
  }
  return lhs << " waiting_on_pushes=" << rhs.waiting_on_pushes
	     << " extent_requested=" << rhs.extent_requested;
}

ECBackend::ECBackend(
  PGBackend::Listener *pg,
  coll_t coll,
  coll_t temp_coll,
  ObjectStore *store,
  CephContext *cct,
  ErasureCodeInterfaceRef ec_impl)
  : PGBackend(pg, store, coll, temp_coll),
    cct(cct),
    ec_impl(ec_impl),
    stripe_width(
      ec_impl->get_chunk_size(4*(2<<10) /* make more flexible */) *
      ec_impl->get_data_chunk_count()),
    stripe_size(ec_impl->get_data_chunk_count()) {}

PGBackend::RecoveryHandle *ECBackend::open_recovery_op()
{
  return new ECRecoveryHandle;
}

struct RecoveryMessages {
  map<hobject_t,
      list<boost::tuple<uint64_t, uint64_t, set<shard_id_t> > > > to_read;
  set<hobject_t> xattrs_to_read;

  void read(
    const hobject_t &hoid, uint64_t off, uint64_t len,
    const set<shard_id_t> &need) {
    to_read[hoid].push_back(boost::make_tuple(off, len, need));
  }
  void fetch_xattrs(
    const hobject_t &hoid) {
    to_read[hoid];
    xattrs_to_read.insert(hoid);
  }

  map<pg_shard_t, vector<PushOp> > pushes;
  map<pg_shard_t, vector<PushReplyOp> > push_replies;
  ObjectStore::Transaction *t;
  RecoveryMessages() : t(new ObjectStore::Transaction) {}
  ~RecoveryMessages() { assert(!t); }
};

void ECBackend::handle_recovery_push(
  PushOp &op,
  RecoveryMessages *m)
{
  bool oneshot = op.before_progress.first && op.after_progress.data_complete;
  coll_t tcoll = oneshot ? coll : get_temp_coll(m->t);
  uint64_t start = op.data_included.range_start();
  uint64_t end = op.data_included.range_end();
  assert(op.data.length() == (end - start));

  m->t->write(
    tcoll,
    ghobject_t(
      op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    start,
    op.data.length(),
    op.data);

  if (op.before_progress.first) {
    if (!oneshot)
      add_temp_obj(op.soid);
    assert(op.attrset.count(string("_")));
    m->t->setattrs(
      tcoll,
      ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      op.attrset);
  }

  if (op.after_progress.data_complete && !oneshot) {
    clear_temp_obj(op.soid);
    m->t->collection_move(
      coll,
      tcoll,
      ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }
  if (op.before_progress.first && get_parent()->pgb_is_primary()) {
    get_parent()->on_local_recover_start(
      op.soid,
      m->t);
  }
  if (op.after_progress.data_complete && !(get_parent()->pgb_is_primary())) {
    get_parent()->on_local_recover(
      op.soid,
      object_stat_sum_t(),
      op.recovery_info,
      ObjectContextRef(),
      m->t);
  }
  if (get_parent()->pgb_is_primary() &&
      op.after_progress.data_complete) {
    assert(recovery_ops.count(op.soid));
    assert(recovery_ops[op.soid].obc);
    object_stat_sum_t stats;
    stats.num_objects_recovered = 1;
    stats.num_bytes_recovered = recovery_ops[op.soid].obc->obs.oi.size;
    get_parent()->on_local_recover(
      op.soid,
      stats,
      op.recovery_info,
      recovery_ops[op.soid].obc,
      m->t);
  }
  m->push_replies[get_parent()->primary_shard()].push_back(PushReplyOp());
  m->push_replies[get_parent()->primary_shard()].back().soid = op.soid;
}

void ECBackend::handle_recovery_push_reply(
  PushReplyOp &op,
  pg_shard_t from,
  RecoveryMessages *m)
{
  if (!recovery_ops.count(op.soid))
    return;
  RecoveryOp &rop = recovery_ops[op.soid];
  assert(rop.waiting_on_pushes.count(from));
  rop.waiting_on_pushes.erase(from);
  continue_recovery_op(rop, m);
}

static ostream &operator<<(ostream &lhs, const map<shard_id_t, bufferlist> &rhs)
{
  lhs << "[";
  for (map<shard_id_t, bufferlist>::const_iterator i = rhs.begin();
       i != rhs.end();
       ++i) {
    if (i != rhs.begin())
      lhs << ", ";
    lhs << make_pair((unsigned)i->first, i->second.length());
  }
  return lhs << "]";
}

void ECBackend::handle_recovery_read_complete(
  const hobject_t &hoid,
  list<boost::tuple<uint64_t, uint64_t, map<shard_id_t, bufferlist> > > &to_read,
  map<string, bufferlist> *attrs,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": returned " << hoid << " "
	   << "(" << to_read.back().get<0>()
	   << ", " << to_read.back().get<1>()
	   << ", " << to_read.back().get<2>()
	   << ")"
	   << dendl;
  assert(to_read.size() == 1);
  assert(recovery_ops.count(hoid));
  RecoveryOp &op = recovery_ops[hoid];
  op.returned_data.swap(to_read.front().get<2>());
  if (attrs) {
    op.xattrs.swap(*attrs);
    op.obc = get_parent()->get_obc(hoid, op.xattrs);
  }
  assert(op.xattrs.size());
  assert(op.obc);
  continue_recovery_op(op, m);
}

struct OnRecoveryReadComplete : public Context {
  map<
    hobject_t,
    list<boost::tuple<uint64_t, uint64_t, map<shard_id_t, bufferlist> > >
    > data;
  map<hobject_t, map<string, bufferlist> > attrs;
  ECBackend *pg;
  OnRecoveryReadComplete(ECBackend *pg) : pg(pg) {}
  void finish(int) {
    RecoveryMessages rm;
    for (map<
	   hobject_t,
	   list<boost::tuple<uint64_t, uint64_t, map<shard_id_t, bufferlist> > >
	   >::iterator i =
	   data.begin();
	 i != data.end();
	 data.erase(i++)) {
      map<hobject_t, map<string, bufferlist> >::iterator aiter = attrs.find(
	i->first);
      pg->handle_recovery_read_complete(
	i->first,
	i->second,
	aiter == attrs.end() ? NULL : &(aiter->second),
	&rm);
    }
    pg->dispatch_recovery_messages(rm);
  }
};

void ECBackend::dispatch_recovery_messages(RecoveryMessages &m)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = m.pushes.begin();
       i != m.pushes.end();
       m.pushes.erase(i++)) {
    MOSDPGPush *msg = new MOSDPGPush();
    msg->map_epoch = get_parent()->get_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->pushes.swap(i->second);
    msg->compute_cost(cct);
    get_parent()->send_message(
      i->first.osd,
      msg);
  }
  for (map<pg_shard_t, vector<PushReplyOp> >::iterator i =
	 m.push_replies.begin();
       i != m.push_replies.end();
       m.push_replies.erase(i++)) {
    MOSDPGPushReply *msg = new MOSDPGPushReply();
    msg->map_epoch = get_parent()->get_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->replies.swap(i->second);
    msg->compute_cost(cct);
    get_parent()->send_message(
      i->first.osd,
      msg);
  }
  get_parent()->queue_transaction(m.t);
  m.t = NULL;
  if (m.to_read.empty())
    return;
  OnRecoveryReadComplete *c = new OnRecoveryReadComplete(this);
  list<
    pair<
      hobject_t,
      boost::tuple<uint64_t, uint64_t, bufferlist*,
		   map<shard_id_t, bufferlist*> >
      >
    > to_read;
  map<hobject_t, map<string, bufferlist> *> xattrs_to_read;
  for (map<
	 hobject_t,
	 list<boost::tuple<uint64_t, uint64_t, set<shard_id_t> > >
	 >::iterator i =
	 m.to_read.begin();
       i != m.to_read.end();
       m.to_read.erase(i++)) {
    list<
      boost::tuple<uint64_t, uint64_t, map<shard_id_t, bufferlist> > > &dlist =
      c->data[i->first];
    for (list<boost::tuple<uint64_t, uint64_t, set<shard_id_t> > >::iterator j =
	   i->second.begin();
	 j != i->second.end();
	 i->second.erase(j++)) {
      dlist.push_back(
	boost::make_tuple(
	  j->get<0>(),
	  j->get<1>(),
	  map<shard_id_t, bufferlist>()));
      to_read.push_back(
	make_pair(
	  i->first,
	  boost::make_tuple(
	    j->get<0>(), j->get<1>(),
	    (bufferlist*)NULL, map<shard_id_t, bufferlist*>())));
      for (set<shard_id_t>::iterator k = j->get<2>().begin();
	   k != j->get<2>().end();
	   ++k) {
	dlist.back().get<2>().insert(make_pair(*k, bufferlist()));
	to_read.back().second.get<3>().insert(
	  make_pair(
	    *k,
	    &(dlist.back().get<2>()[*k])));
      }
    }
    if (m.xattrs_to_read.count(i->first)) {
      xattrs_to_read.insert(
	make_pair(
	  i->first,
	  &(c->attrs[i->first])));
    }
  }
  start_read_op(
    to_read,
    xattrs_to_read,
    c,
    OpRequestRef(),
    true);
}

void ECBackend::continue_recovery_op(
  RecoveryOp &op,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": continuing " << op << dendl;
  while (1) {
    switch (op.state) {
    case RecoveryOp::IDLE: {
      // start read
      op.state = RecoveryOp::READING;
      if (op.recovery_progress.first) {
	m->fetch_xattrs(op.hoid);
      }
      assert(!op.recovery_progress.data_complete);
      m->read(op.hoid, op.recovery_progress.data_recovered_to,
	      get_recovery_chunk_size(), op.missing_on_shards);
      op.extent_requested = make_pair(op.recovery_progress.data_recovered_to,
				      get_recovery_chunk_size());
      dout(10) << __func__ << ": IDLE return " << op << dendl;
      return;
    }
    case RecoveryOp::READING: {
      // read completed, start write
      assert(op.xattrs.size());
      assert(op.returned_data.size());
      op.state = RecoveryOp::WRITING;
      ObjectRecoveryProgress after_progress = op.recovery_progress;
      after_progress.data_recovered_to += get_recovery_chunk_size();
      after_progress.first = false;
      if (after_progress.data_recovered_to >= op.obc->obs.oi.size)
	after_progress.data_complete = true;
      for (set<pg_shard_t>::iterator mi = op.missing_on.begin();
	   mi != op.missing_on.end();
	   ++mi) {
	assert(op.returned_data.count(mi->shard));
	m->pushes[*mi].push_back(PushOp());
	PushOp &pop = m->pushes[*mi].back();
	pop.soid = op.hoid;
	pop.version = op.v;
	pop.data = op.returned_data[mi->shard];
	pop.data_included.insert(
	  ECUtil::logical_to_prev_stripe_bound_obj(
	    stripe_size,
	    stripe_width,
	    op.recovery_progress.data_recovered_to),
	  pop.data.length()
	  );
	if (op.recovery_progress.first) {
	  pop.attrset = op.xattrs;
	}
	pop.recovery_info = op.recovery_info;
	pop.before_progress = op.recovery_progress;
	pop.after_progress = after_progress;
	if (*mi != get_parent()->primary_shard())
	  get_parent()->begin_peer_recover(
	    *mi,
	    op.hoid);
      }
      op.waiting_on_pushes = op.missing_on;
      op.recovery_progress = after_progress;
      dout(10) << __func__ << ": READING return " << op << dendl;
      return;
    }
    case RecoveryOp::WRITING: {
      if (op.waiting_on_pushes.empty()) {
	if (op.recovery_progress.data_complete) {
	  op.state = RecoveryOp::COMPLETE;
	  for (set<pg_shard_t>::iterator i = op.missing_on.begin();
	       i != op.missing_on.end();
	       ++i) {
	    if (*i != get_parent()->primary_shard())
	      get_parent()->on_peer_recover(
		*i,
		op.hoid,
		op.recovery_info,
		object_stat_sum_t());
	  }
	  get_parent()->on_global_recover(op.hoid);
	  dout(10) << __func__ << ": WRITING return " << op << dendl;
	  recovery_ops.erase(op.hoid);
	  return;
	} else {
	  op.state = RecoveryOp::IDLE;
	  dout(10) << __func__ << ": WRITING continue " << op << dendl;
	  continue;
	}
      }
      return;
    }
    case RecoveryOp::COMPLETE: {
      assert(0); // should never be called once complete
    };
    default:
      assert(0);
    }
  }
}

void ECBackend::run_recovery_op(
  RecoveryHandle *_h,
  int priority)
{
  ECRecoveryHandle *h = static_cast<ECRecoveryHandle*>(_h);
  RecoveryMessages m;
  for (list<RecoveryOp>::iterator i = h->ops.begin();
       i != h->ops.end();
       ++i) {
    dout(10) << __func__ << ": starting " << *i << dendl;
    assert(!recovery_ops.count(i->hoid));
    RecoveryOp &op = recovery_ops.insert(make_pair(i->hoid, *i)).first->second;
    continue_recovery_op(op, &m);
  }
  dispatch_recovery_messages(m);
  delete _h;
}

void ECBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h)
{
  ECRecoveryHandle *h = static_cast<ECRecoveryHandle*>(_h);
  h->ops.push_back(RecoveryOp());
  h->ops.back().v = v;
  h->ops.back().hoid = hoid;
  h->ops.back().obc = obc;
  h->ops.back().recovery_info.soid = hoid;
  h->ops.back().recovery_info.version = v;
  if (obc) {
    h->ops.back().recovery_info.size = obc->obs.oi.size;
    h->ops.back().recovery_info.oi = obc->obs.oi;
  }
  h->ops.back().recovery_progress.omap_complete = true;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    dout(10) << "checking " << *i << dendl;
    if (hoid > get_parent()->get_shard_info(*i).last_backfill ||
      get_parent()->get_shard_missing(*i).is_missing(hoid)) {
      h->ops.back().missing_on.insert(*i);
      h->ops.back().missing_on_shards.insert(i->shard);
    }
  }
  dout(10) << __func__ << ": built op " << h->ops.back() << dendl;
}

bool ECBackend::can_handle_while_inactive(
  OpRequestRef _op)
{
  switch (_op->get_req()->get_type()) {
  case MSG_OSD_EC_READ:
    return true;
  default:
    return false;
  }
}

bool ECBackend::handle_message(
  OpRequestRef _op)
{
  dout(10) << __func__ << ": " << *_op->get_req() << dendl;
  switch (_op->get_req()->get_type()) {
  case MSG_OSD_EC_WRITE: {
    MOSDECSubOpWrite *op = static_cast<MOSDECSubOpWrite*>(_op->get_req());
    handle_sub_write(op->op.from, _op, op->op);
    return true;
  }
  case MSG_OSD_EC_WRITE_REPLY: {
    MOSDECSubOpWriteReply *op = static_cast<MOSDECSubOpWriteReply*>(
      _op->get_req());
    handle_sub_write_reply(op->op.from, op->op);
    return true;
  }
  case MSG_OSD_EC_READ: {
    MOSDECSubOpRead *op = static_cast<MOSDECSubOpRead*>(_op->get_req());
    MOSDECSubOpReadReply *reply = new MOSDECSubOpReadReply;
    reply->pgid = get_parent()->primary_spg_t();
    reply->map_epoch = get_parent()->get_epoch();
    handle_sub_read(op->op.from, op->op, &(reply->op));
    get_parent()->send_message_osd_cluster(
      op->op.from.osd, reply, get_parent()->get_epoch());
    return true;
  }
  case MSG_OSD_EC_READ_REPLY: {
    MOSDECSubOpReadReply *op = static_cast<MOSDECSubOpReadReply*>(
      _op->get_req());
    handle_sub_read_reply(op->op.from, op->op);
    return true;
  }
  case MSG_OSD_PG_PUSH: {
    MOSDPGPush *op = static_cast<MOSDPGPush *>(_op->get_req());
    RecoveryMessages rm;
    for (vector<PushOp>::iterator i = op->pushes.begin();
	 i != op->pushes.end();
	 ++i) {
      handle_recovery_push(*i, &rm);
    }
    dispatch_recovery_messages(rm);
    return true;
  }
  case MSG_OSD_PG_PUSH_REPLY: {
    MOSDPGPushReply *op = static_cast<MOSDPGPushReply *>(_op->get_req());
    RecoveryMessages rm;
    for (vector<PushReplyOp>::iterator i = op->replies.begin();
	 i != op->replies.end();
	 ++i) {
      handle_recovery_push_reply(*i, op->from, &rm);
    }
    dispatch_recovery_messages(rm);
    return true;
  }
  default:
    return false;
  }
  return false;
}

struct SubWriteCommitted : public Context {
  ECBackend *pg;
  OpRequestRef msg;
  tid_t tid;
  eversion_t version;
  SubWriteCommitted(
    ECBackend *pg,
    OpRequestRef msg,
    tid_t tid,
    eversion_t version)
    : pg(pg), msg(msg), tid(tid), version(version) {}
  void finish(int) {
    msg->mark_event("sub_op_committed");
    pg->sub_write_committed(tid, version);
  }
};
void ECBackend::sub_write_committed(
  tid_t tid, eversion_t version) {
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.tid = tid;
    reply.committed = true;
    reply.from = get_parent()->whoami_shard();
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply);
  } else {
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = get_parent()->get_epoch();
    r->op.tid = tid;
    r->op.committed = true;
    r->op.from = get_parent()->whoami_shard();
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, get_parent()->get_epoch());
  }
}

struct SubWriteApplied : public Context {
  ECBackend *pg;
  OpRequestRef msg;
  tid_t tid;
  eversion_t version;
  SubWriteApplied(
    ECBackend *pg,
    OpRequestRef msg,
    tid_t tid,
    eversion_t version)
    : pg(pg), msg(msg), tid(tid), version(version) {}
  void finish(int) {
    msg->mark_event("sub_op_applied");
    pg->sub_write_applied(tid, version);
  }
};
void ECBackend::sub_write_applied(
  tid_t tid, eversion_t version) {
  parent->op_applied(version);
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.from = get_parent()->whoami_shard();
    reply.tid = tid;
    reply.applied = true;
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply);
  } else {
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = get_parent()->get_epoch();
    r->op.from = get_parent()->whoami_shard();
    r->op.tid = tid;
    r->op.applied = true;
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, get_parent()->get_epoch());
  }
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  Context *on_local_applied_sync)
{
  msg->mark_started();
  assert(!get_parent()->get_log().get_missing().is_missing(op.soid));
  if (!get_parent()->pgb_is_primary())
    get_parent()->update_stats(op.stats);
  ObjectStore::Transaction *localt = new ObjectStore::Transaction;
  get_parent()->log_operation(
    op.log_entries,
    op.trim_to,
    !(op.t.empty()),
    localt);
  localt->append(op.t);
  if (on_local_applied_sync) {
    dout(10) << "Queueing onreadable_sync: " << on_local_applied_sync << dendl;
    localt->register_on_applied_sync(on_local_applied_sync);
  }
  localt->register_on_commit(
    get_parent()->bless_context(
      new SubWriteCommitted(this, msg, op.tid, op.at_version)));
  localt->register_on_applied(
    get_parent()->bless_context(
      new SubWriteApplied(this, msg, op.tid, op.at_version)));
  get_parent()->queue_transaction(localt, msg);
}

void ECBackend::handle_sub_read(
  pg_shard_t from,
  ECSubRead &op,
  ECSubReadReply *reply)
{
  for(list<pair<hobject_t, pair<uint64_t, uint64_t> > >::iterator i =
	op.to_read.begin();
	i != op.to_read.end();
	++i) {
    bufferlist bl;
    int r = store->read(
      i->first.is_temp() ? temp_coll : coll,
      ghobject_t(
	i->first, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      i->second.first,
      i->second.second,
      bl,
      false);
    assert(r >= 0); // TODOSAM: add error pathway
    reply->buffers_read.push_back(
      make_pair(
	i->first,
	make_pair(
	  i->second.first,
	  bl)
	)
      );
  }
  for (set<hobject_t>::iterator i = op.attrs_to_read.begin();
       i != op.attrs_to_read.end();
       ++i) {
    dout(10) << __func__ << ": fulfilling attr request on "
	     << *i << dendl;
    int r = store->getattrs(
      i->is_temp() ? temp_coll : coll,
      ghobject_t(
	*i, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      reply->attrs_read[*i]);
    assert(r >= 0); // TODOSAM: add error pathway
    assert(reply->attrs_read[*i].count("_"));
  }
  reply->from = get_parent()->whoami_shard();
  reply->tid = op.tid;
}

void ECBackend::handle_sub_write_reply(
  pg_shard_t from,
  ECSubWriteReply &op)
{
  map<tid_t, Op>::iterator i = tid_to_op_map.find(op.tid);
  assert(i != tid_to_op_map.end());
  if (op.committed) {
    assert(i->second.pending_commit.count(from));
    i->second.pending_commit.erase(from);
  }
  if (op.applied) {
    assert(i->second.pending_apply.count(from));
    i->second.pending_apply.erase(from);
  }
  check_op(&(i->second));
}

void ECBackend::handle_sub_read_reply(
  pg_shard_t from,
  ECSubReadReply &op)
{
  map<tid_t, ReadOp>::iterator iter = tid_to_read_map.find(op.tid);
  if (iter == tid_to_read_map.end()) {
    //canceled
    return;
  }
  assert(iter->second.in_progress.count(from));
  iter->second.complete[from].swap(op.buffers_read);
  iter->second.in_progress.erase(from);

  dout(10) << __func__ << ": reply " << op << dendl;

  map<pg_shard_t, set<tid_t> >::iterator siter = shard_to_read_map.find(from);
  assert(siter != shard_to_read_map.end());
  assert(siter->second.count(op.tid));
  siter->second.erase(op.tid);

  for (map<hobject_t, map<string, bufferlist> >::iterator i =
	 op.attrs_read.begin();
       i != op.attrs_read.end();
       ++i) {
      map<hobject_t, map<string, bufferlist>*>::iterator j =
	iter->second.attrs_to_read.find(i->first);
      assert(j != iter->second.attrs_to_read.end());
      *(j->second) = i->second;
  }

  ReadOp &readop = iter->second;
  if (!readop.in_progress.empty()) {
    dout(10) << __func__ << " readop not complete: " << readop << dendl;
    return;
  }

  // done
  dout(10) << __func__ << " completing op " << readop << dendl;
  map<pg_shard_t,
      list<pair<hobject_t, pair<uint64_t, bufferlist> > >::iterator
      > res_iters;
  list<
    pair<
      hobject_t,
      boost::tuple<
	uint64_t, uint64_t, bufferlist*, map<shard_id_t, bufferlist*> >
      >
    >::iterator out_iter;

  for (map<pg_shard_t,
	   list<pair<hobject_t, pair<uint64_t, bufferlist> > >
	 >::iterator i = readop.complete.begin();
       i != readop.complete.end();
       ++i) {
    assert(i->second.size() == readop.to_read.size());
    res_iters.insert(make_pair(i->first, i->second.begin()));
  }
  out_iter = readop.to_read.begin();

  while (true) {
    if (res_iters.begin()->second == readop.complete.begin()->second.end())
      break;
    assert(out_iter != readop.to_read.end());
    dout(10) << __func__ << ": reconstructing "
	     << make_pair(out_iter->second.get<0>(), out_iter->second.get<1>())
	     << " for object " << out_iter->first << dendl;
    uint64_t off(res_iters.begin()->second->second.first);
    hobject_t hoid(res_iters.begin()->second->first);
    map<int, bufferlist> chunks;
    for (map<pg_shard_t,
	   list<pair<hobject_t, pair<uint64_t, bufferlist> > >::iterator
	   >::iterator i = res_iters.begin();
	 i != res_iters.end();
	 ++i) {
      assert(i->second->first == hoid);
      assert(i->second->second.first == off);
      chunks[i->first.shard].claim(i->second->second.second);
      ++(i->second);
    }
    if (out_iter->second.get<2>()) {
      bufferlist decoded;
      int r = ECUtil::decode(
	stripe_size, stripe_width, ec_impl, chunks,
	&decoded);
      assert(r == 0);
      out_iter->second.get<2>()->substr_of(
	decoded,
	out_iter->second.get<0>() - ECUtil::logical_to_prev_stripe_bound_obj(
	  stripe_size, stripe_width, out_iter->second.get<0>()),
	out_iter->second.get<1>());
    }
    if (out_iter->second.get<3>().size()) {
      assert(out_iter->second.get<0>() % stripe_width == 0);
      assert(out_iter->second.get<1>() % stripe_width == 0);
      map<int, bufferlist*> out(
	out_iter->second.get<3>().begin(),
	out_iter->second.get<3>().end());
      int r = ECUtil::decode(
	stripe_size,
	stripe_width,
	ec_impl,
	chunks,
	out);
      assert(r == 0);
      for (map<shard_id_t, bufferlist*>::iterator i =
	     out_iter->second.get<3>().begin();
	   i != out_iter->second.get<3>().end();
	   ++i) {
	assert(i->second->length() == chunks.begin()->second.length());
      }
    }
    ++out_iter;
  }
  readop.on_complete->complete(0);
  readop.on_complete = NULL;
  assert(readop.in_progress.empty());
  tid_to_read_map.erase(readop.tid);
}

void ECBackend::check_recovery_sources(const OSDMapRef osdmap)
{
  set<tid_t> tids_to_restart;
  for (map<pg_shard_t, set<tid_t> >::iterator i = shard_to_read_map.begin();
       i != shard_to_read_map.end();
       ++i) {
    if (osdmap->is_down(i->first.osd)) {
      tids_to_restart.insert(i->second.begin(), i->second.end());
    }
  }
  for (set<tid_t>::iterator i = tids_to_restart.begin();
       i != tids_to_restart.end();
       ++i) {
    map<tid_t, ReadOp>::iterator j = tid_to_read_map.find(*i);
    assert(j != tid_to_read_map.end());
    restart_read_op(j->second);
  }
}

void ECBackend::_on_change(ObjectStore::Transaction *t)
{
  writing.clear();
  tid_to_op_map.clear();
  tid_to_read_map.clear();
  shard_to_read_map.clear();
  clear_state();
}

void ECBackend::clear_state()
{
  recovery_ops.clear();
}

void ECBackend::on_flushed()
{
}


void ECBackend::dump_recovery_info(Formatter *f) const
{
}

PGBackend::PGTransaction *ECBackend::get_transaction()
{
  return new ECTransaction;
}

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const eversion_t &at_version,
  PGTransaction *_t,
  const eversion_t &trim_to,
  vector<pg_log_entry_t> &log_entries,
  Context *on_local_applied_sync,
  Context *on_all_applied,
  Context *on_all_commit,
  tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef client_op
  )
{
  assert(!tid_to_op_map.count(tid));
  Op *op = &(tid_to_op_map[tid]);
  op->hoid = hoid;
  op->version = at_version;
  op->trim_to = trim_to;
  op->log_entries.swap(log_entries);
  op->on_local_applied_sync = on_local_applied_sync;
  op->on_all_applied = on_all_applied;
  op->on_all_commit = on_all_commit;
  op->tid = tid;
  op->reqid = reqid;
  op->client_op = client_op;

  op->t = static_cast<ECTransaction*>(_t);
  dout(10) << __func__ << ": op " << *op << " waiting stripe_width: "
	   << stripe_width << " stripe_size: " << stripe_size << dendl;
  start_write(op);
  writing.push_back(op);
  dout(10) << "onreadable_sync: " << op->on_local_applied_sync << dendl;
}

int ECBackend::get_min_avail_to_read(
  const hobject_t &hoid,
  set<pg_shard_t> *to_read)
{
  set<int> want;
  for (unsigned i = 0; i < ec_impl->get_data_chunk_count(); ++i)
    want.insert(i);
  return get_min_avail_to_read_shards(hoid, want, to_read);
}

int ECBackend::get_min_avail_to_read_shards(
  const hobject_t &hoid,
  const set<int> &want,
  set<pg_shard_t> *to_read)
{
  map<hobject_t, set<pg_shard_t> >::const_iterator miter =
    get_parent()->get_missing_loc_shards().find(hoid);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;

  for (set<pg_shard_t>::const_iterator i = 
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (!missing.is_missing(hoid)) {
      assert(!have.count(i->shard));
      have.insert(i->shard);
      assert(!shards.count(i->shard));
      shards.insert(make_pair(i->shard, *i));
    }
  }
  
  for (set<pg_shard_t>::const_iterator i = 
	 get_parent()->get_backfill_shards().begin();
       i != get_parent()->get_backfill_shards().end();
       ++i) {
    if (have.count(i->shard)) {
      assert(shards.count(i->shard));
      continue;
    }
    assert(!shards.count(i->shard));
    const pg_info_t &info = get_parent()->get_shard_info(*i);
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (hoid < info.last_backfill && !missing.is_missing(hoid)) {
      have.insert(i->shard);
      shards.insert(make_pair(i->shard, *i));
    }
  }

  if (miter != get_parent()->get_missing_loc_shards().end()) {
    for (set<pg_shard_t>::iterator i = miter->second.begin();
	 i != miter->second.end();
	 ++i) {
      have.insert(i->shard);
      shards.insert(make_pair(i->shard, *i));
    }
  }

  set<int> need;
  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0)
    return r;

  if (!to_read)
    return 0;

  for (set<int>::iterator i = need.begin();
       i != need.end();
       ++i) {
    assert(shards.count(*i));
    to_read->insert(shards[*i]);
  }
  return 0;
}

void ECBackend::start_read_op(
  const list<
    pair<
      hobject_t,
      boost::tuple<
	uint64_t, uint64_t, bufferlist*, map<shard_id_t, bufferlist*> >
      >
    > &to_read,
  const map<hobject_t, map<string, bufferlist> *> &attrs_to_read,
  Context *onfinish,
  OpRequestRef _op,
  bool for_recovery)
{
  tid_t tid = get_parent()->get_tid();
  assert(!tid_to_read_map.count(tid));
  ReadOp &op(tid_to_read_map[tid]);
  op.to_read = to_read;
  op.on_complete = onfinish;
  op.attrs_to_read = attrs_to_read;
  op.op = _op;
  op.tid = tid;
  dout(10) << __func__ << ": starting " << op << dendl;

  map<pg_shard_t, ECSubRead> messages;
  for (list<
	 pair<
	   hobject_t,
	   boost::tuple<
	     uint64_t, uint64_t, bufferlist*, map<shard_id_t, bufferlist*> >
	   >
	 >::const_iterator i = to_read.begin();
       i != to_read.end();
       ++i) {
    uint64_t obj_offset =
      ECUtil::logical_to_prev_stripe_bound_obj(
	stripe_size, stripe_width,
	i->second.get<0>());
    uint64_t obj_len =
      ECUtil::logical_to_next_stripe_bound_obj(
	stripe_size, stripe_width,
	i->second.get<1>());
    set<pg_shard_t> min;
    if (i->second.get<2>()) {
      int r = get_min_avail_to_read(
	i->first,
	&min);
      if (r != 0 && for_recovery) {
	get_parent()->cancel_pull(i->first);
	continue;
      } else if (r != 0) {
	assert(0); // caller should have ensured that the object is readable
      }
    } else if (i->second.get<3>().size()) {
      assert(i->second.get<0>() % stripe_width == 0);
      assert(i->second.get<1>() % stripe_width == 0);
      set<int> needed;
      for (map<shard_id_t, bufferlist*>::const_iterator j =
	     i->second.get<3>().begin();
	   j != i->second.get<3>().end();
	   ++j) {
	needed.insert(j->first);
      }
      int r = get_min_avail_to_read_shards(
	i->first,
	needed,
	&min);
      if (r != 0 && for_recovery) {
	get_parent()->cancel_pull(i->first);
	continue;
      } else if (r != 0) {
	assert(0); // caller should have ensured that the object is readable
      }
    } else {
      assert(0); // once there is a caller which only needs xattrs, fix this
    }
    bool must_request_attrs = attrs_to_read.count(i->first);
    for (set<pg_shard_t>::iterator j = min.begin();
	 j != min.end();
	 ++j) {
      messages[*j].to_read.push_back(
	make_pair(
	  i->first,
	  make_pair(obj_offset, obj_len)));
      if (must_request_attrs) {
	dout(10) << __func__ << ": requesting attrs from "
		 << *j << " for " << i->first << dendl;
	messages[*j].attrs_to_read.insert(i->first);
	must_request_attrs = false;
      }
    }
  }
  for (map<pg_shard_t, ECSubRead>::iterator i = messages.begin();
       i != messages.end();
       ++i) {
    op.in_progress.insert(i->first);
    shard_to_read_map[i->first].insert(op.tid);
    i->second.tid = tid;
    MOSDECSubOpRead *msg = new MOSDECSubOpRead;
    msg->pgid = spg_t(
      get_parent()->whoami_spg_t().pgid,
      i->first.shard);
    msg->map_epoch = get_parent()->get_epoch();
    msg->op = i->second;
    msg->op.from = get_parent()->whoami_shard();
    msg->op.tid = tid;
    get_parent()->send_message_osd_cluster(
      i->first.osd,
      msg,
      get_parent()->get_epoch());
  }
  dout(10) << __func__ << ": started " << op << dendl;
}

void ECBackend::cancel_read_op(
  tid_t tid)
{
  assert(tid_to_read_map.count(tid));
  ReadOp &op = tid_to_read_map[tid];
  for (set<pg_shard_t>::iterator i = op.in_progress.begin();
       i != op.in_progress.end();
       ++i) {
    map<pg_shard_t, set<tid_t> >::iterator siter = shard_to_read_map.find(*i);
    assert(siter != shard_to_read_map.end());
    assert(siter->second.count(tid));
    siter->second.erase(tid);
    if (siter->second.empty())
      shard_to_read_map.erase(siter);
  }
  tid_to_read_map.erase(tid);
}

void ECBackend::restart_read_op(
  ReadOp &op)
{
  start_read_op(
    op.to_read,
    op.attrs_to_read,
    op.on_complete,
    op.op,
    true);
  cancel_read_op(op.tid);
}

void ECBackend::check_op(Op *op)
{
  if (op->pending_apply.empty() && op->on_all_applied) {
    dout(10) << __func__ << " Calling on_all_applied on " << *op << dendl;
    op->on_all_applied->complete(0);
    op->on_all_applied = 0;
  }
  if (op->pending_commit.empty() && op->on_all_commit) {
    dout(10) << __func__ << " Calling on_all_commit on " << *op << dendl;
    op->on_all_commit->complete(0);
    op->on_all_commit = 0;
  }
  if (op->pending_apply.empty() && op->pending_commit.empty()) {
    // done!
    assert(writing.front() == op);
    dout(10) << __func__ << " Completing " << *op << dendl;
    writing.pop_front();
    tid_to_op_map.erase(op->tid);
  }
  for (map<tid_t, Op>::iterator i = tid_to_op_map.begin();
       i != tid_to_op_map.end();
       ++i) {
    dout(20) << __func__ << " tid " << i->first <<": " << i->second << dendl;
  }
}

void ECBackend::start_write(Op *op) {
  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    if (get_parent()->should_send_op(*i, op->hoid))
      trans[i->shard];
  }
  op->t->generate_transactions(
    ec_impl,
    get_parent()->get_info().pgid.pgid,
    stripe_width,
    stripe_size,
    &trans,
    &(op->temp_added),
    &(op->temp_cleared));

  dout(10) << "onreadable_sync: " << op->on_local_applied_sync << dendl;

  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    op->pending_apply.insert(*i);
    op->pending_commit.insert(*i);
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    assert(iter != trans.end());
    bool should_send = get_parent()->should_send_op(*i, op->hoid);
    pg_stat_t stats =
      should_send ?
      get_info().stats :
      parent->get_shard_info().find(*i)->second.stats;
	
    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op->tid,
      op->reqid,
      op->hoid,
      stats,
      should_send ? iter->second : ObjectStore::Transaction(),
      op->version,
      op->trim_to,
      op->log_entries,
      op->temp_added,
      op->temp_cleared);
    if (*i == get_parent()->whoami_shard()) {
      handle_sub_write(
	get_parent()->whoami_shard(),
	op->client_op,
	sop,
	op->on_local_applied_sync);
      op->on_local_applied_sync = 0;
    } else {
      MOSDECSubOpWrite *r = new MOSDECSubOpWrite(sop);
      r->pgid = spg_t(get_parent()->primary_spg_t().pgid, i->shard);
      r->map_epoch = get_parent()->get_epoch();
      get_parent()->send_message_osd_cluster(
	i->osd, r, get_parent()->get_epoch());
    }
  }
}

int ECBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  bufferlist *bl)
{
  return -EOPNOTSUPP;
}

struct CallClientContexts : public Context {
  list<pair<pair<uint64_t, uint64_t>,
	    pair<bufferlist*, Context*> > > to_read;
  Context *c;
  CallClientContexts(
    const list<pair<pair<uint64_t, uint64_t>,
		    pair<bufferlist*, Context*> > > &to_read,
    Context *c)
    : to_read(to_read), c(c) {}
  void finish(int r) {
    for (list<pair<pair<uint64_t, uint64_t>,
		   pair<bufferlist*, Context*> > >::iterator i = to_read.begin();
	 i != to_read.end();
	 to_read.erase(i++)) {
      if (i->second.second) {
	if (r == 0) {
	  i->second.second->complete(i->second.first->length());
	} else {
	  i->second.second->complete(r);
	}
      }
    }
    c->complete(r);
    c = NULL;
  }
  ~CallClientContexts() {
    for (list<pair<pair<uint64_t, uint64_t>,
		   pair<bufferlist*, Context*> > >::iterator i = to_read.begin();
	 i != to_read.end();
	 to_read.erase(i++)) {
      delete i->second.second;
    }
    delete c;
  }
};

void ECBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<pair<uint64_t, uint64_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete)
{
  list<
    pair<
      hobject_t,
      boost::tuple<
	uint64_t, uint64_t, bufferlist*, map<shard_id_t, bufferlist* > >
      >
    > for_read_op;
  for (list<pair<pair<uint64_t, uint64_t>,
		 pair<bufferlist*, Context*> > >::const_iterator i =
	 to_read.begin();
       i != to_read.end();
       ++i) {
    for_read_op.push_back(
      make_pair(
	hoid,
	boost::make_tuple(
	  i->first.first, i->first.second, i->second.first,
	  map<shard_id_t, bufferlist*>())));
  }
  start_read_op(
    for_read_op,
    map<hobject_t, map<string, bufferlist>*>(),
    new CallClientContexts(to_read, on_complete),
    OpRequestRef(),
    true);
  return;
}

void ECBackend::rollback_append(
  const hobject_t &hoid,
  uint64_t old_size,
  ObjectStore::Transaction *t)
{
  assert(old_size % stripe_width == 0);
  t->truncate(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    ECUtil::logical_to_prev_stripe_bound_obj(stripe_size, stripe_width, old_size));
}
