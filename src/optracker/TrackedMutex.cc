// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "TrackedMutex.h"

static const string UNCONTENDED_LOCKED("uncontended-lock");
static const string CONTENDED_WAITING("contended-waiting");
static const string CONTENDED_LOCKED("contended-locked");
static const string UNLOCK("unlock");

void TrackedMutex::Unlock(
  const TrackedOp *op) {
  log_event(op, UNLOCK);
  return lock.Unlock();
}

void TrackedMutex::Lock(
  const TrackedOp *op,
  bool no_lockdep) {
  if (lock.TryLock()) {
    log_event(op, UNCONTENDED_LOCKED);
  } else {
    log_event(op, CONTENDED_WAITING);
    lock.Lock(no_lockdep);
    log_event(op, CONTENDED_LOCKED);
  }
  return;
}
