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
#include <vector>
#include <sstream>

#include "ECBackend.h"
#include "ECUtil.h"
#include "os/ObjectStore.h"

struct TransGenerator : public boost::static_visitor<void> {
  typedef void result_type;

  ErasureCodeInterfaceRef &ecimpl;
  const pg_t pgid;
  const uint64_t stripe_width;
  const uint64_t stripe_size;
  map<shard_id_t, ObjectStore::Transaction> *trans;
  set<int> want;
  set<hobject_t> *temp_added;
  set<hobject_t> *temp_removed;
  stringstream *out;
  TransGenerator(
    ErasureCodeInterfaceRef &ecimpl,
    pg_t pgid,
    const uint64_t stripe_width,
    const uint64_t stripe_size,
    map<shard_id_t, ObjectStore::Transaction> *trans,
    set<hobject_t> *temp_added,
    set<hobject_t> *temp_removed,
    stringstream *out)
    : ecimpl(ecimpl), pgid(pgid),
      stripe_width(stripe_width), stripe_size(stripe_size),
      trans(trans),
      temp_added(temp_added), temp_removed(temp_removed),
      out(out) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      want.insert(i->first);
    }
  }

  coll_t get_coll_ct(shard_id_t shard, const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_removed->erase(hoid);
      temp_added->insert(hoid);
    }
    return get_coll(shard, hoid);
  }
  coll_t get_coll_rm(shard_id_t shard, const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_added->erase(hoid);
      temp_removed->insert(hoid);
    }
    return get_coll(shard, hoid);
  }
  coll_t get_coll(shard_id_t shard, const hobject_t &hoid) {
    if (hoid.is_temp())
      return coll_t::make_temp_coll(spg_t(pgid, shard));
    else
      return coll_t(spg_t(pgid, shard));
  }

  void operator()(const ECTransaction::AppendOp &op) {
    uint64_t offset = op.off;
    bufferlist bl(op.bl);
    assert(offset % stripe_width == 0);
    map<int, bufferlist> buffers;

    // align
    bl.append_zero(stripe_width - ((offset + bl.length()) % stripe_width));
    int r = ECUtil::encode(
      stripe_size, stripe_width, ecimpl, bl, want, &buffers);
    assert(r == 0);
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      assert(buffers.count(i->first));
      bufferlist &enc_bl = buffers[i->first];
      i->second.write(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	ECUtil::logical_to_prev_stripe_bound_obj(
	  stripe_size, stripe_width, offset),
	enc_bl.length(),
	enc_bl);
    }
  }
  void operator()(const ECTransaction::CloneOp &op) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.clone(
	get_coll_ct(i->first, op.source),
	ghobject_t(op.source, ghobject_t::NO_GEN, i->first),
	ghobject_t(op.target, ghobject_t::NO_GEN, i->first));
    }
  }
  void operator()(const ECTransaction::RenameOp &op) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.collection_move_rename(
	get_coll_rm(i->first, op.source),
	ghobject_t(op.source, ghobject_t::NO_GEN, i->first),
	get_coll_ct(i->first, op.destination),
	ghobject_t(op.destination, ghobject_t::NO_GEN, i->first));
    }
  }
  void operator()(const ECTransaction::StashOp &op) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      coll_t cid(get_coll_rm(i->first, op.oid));
      i->second.collection_move_rename(
	cid,
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	cid,
	ghobject_t(op.oid, op.version, i->first));
    }
  }
  void operator()(const ECTransaction::RemoveOp &op) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.remove(
	get_coll_rm(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first));
    }
  }
  void operator()(const ECTransaction::SetAttrsOp &op) {
    map<string, bufferlist> attrs(op.attrs);
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.setattrs(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	attrs);
    }
  }
  void operator()(const ECTransaction::RmAttrOp &op) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.rmattr(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	op.key);
    }
  }
  void operator()(const ECTransaction::NoOp &op) {}
};


void ECTransaction::generate_transactions(
  ErasureCodeInterfaceRef &ecimpl,
  pg_t pgid,
  uint64_t stripe_width,
  uint64_t stripe_size,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  set<hobject_t> *temp_added,
  set<hobject_t> *temp_removed,
  stringstream *out) const
{
  TransGenerator gen(
    ecimpl,
    pgid,
    stripe_width,
    stripe_size,
    transactions,
    temp_added,
    temp_removed,
    out);
  visit(gen);
}
