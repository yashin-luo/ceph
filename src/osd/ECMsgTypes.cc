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

#include "ECMsgTypes.h"

void ECSubWrite::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(from, bl);
  ::encode(tid, bl);
  ::encode(reqid, bl);
  ::encode(soid, bl);
  ::encode(stats, bl);
  ::encode(t, bl);
  ::encode(at_version, bl);
  ::encode(trim_to, bl);
  ::encode(log_entries, bl);
  ::encode(temp_added, bl);
  ::encode(temp_removed, bl);
  ENCODE_FINISH(bl);
}

void ECSubWrite::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(from, bl);
  ::decode(tid, bl);
  ::decode(reqid, bl);
  ::decode(soid, bl);
  ::decode(stats, bl);
  ::decode(t, bl);
  ::decode(at_version, bl);
  ::decode(trim_to, bl);
  ::decode(log_entries, bl);
  ::decode(temp_added, bl);
  ::decode(temp_removed, bl);
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubWrite &rhs)
{
  return lhs
    << "ECSubWrite(tid=" << rhs.tid
    << ", reqid=" << rhs.reqid
    << ", at_version=" << rhs.at_version
    << ", trim_to=" << rhs.trim_to << ")";
}

void ECSubWriteReply::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(from, bl);
  ::encode(tid, bl);
  ::encode(committed, bl);
  ::encode(applied, bl);
  ENCODE_FINISH(bl);
}

void ECSubWriteReply::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(from, bl);
  ::decode(tid, bl);
  ::decode(committed, bl);
  ::decode(applied, bl);
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubWriteReply &rhs)
{
  return lhs
    << "ECSubWriteReply(tid=" << rhs.tid
    << ", committed=" << rhs.committed
    << ", applied=" << rhs.applied << ")";
}

void ECSubRead::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(from, bl);
  ::encode(tid, bl);
  ::encode(to_read, bl);
  ::encode(attrs_to_read, bl);
  ENCODE_FINISH(bl);
}

void ECSubRead::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(from, bl);
  ::decode(tid, bl);
  ::decode(to_read, bl);
  ::decode(attrs_to_read, bl);
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubRead &rhs)
{
  return lhs
    << "ECSubRead(tid=" << rhs.tid
    << ", to_read=" << rhs.to_read
    << ", attrs_to_read=" << rhs.attrs_to_read << ")";
}

void ECSubReadReply::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(from, bl);
  ::encode(tid, bl);
  ::encode(buffers_read, bl);
  ::encode(attrs_read, bl);
  ENCODE_FINISH(bl);
}

void ECSubReadReply::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(from, bl);
  ::decode(tid, bl);
  ::decode(buffers_read, bl);
  ::decode(attrs_read, bl);
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubReadReply &rhs)
{
  return lhs
    << "ECSubReadReply(tid=" << rhs.tid
    << ", attrs_read=" << rhs.attrs_read.size()
    << ")";
}
