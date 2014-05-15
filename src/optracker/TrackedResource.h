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

#ifndef CEPH_TRACKED_RESOURCE_H
#define CEPH_TRACKED_RESOURCE_H

#include "TrackedOp.h"
#include "common/Formatter.h"

struct tracked_res_t {
  const char *type_id;
  const char *class_id;
  const char *inst_id;
  tracked_res_t(
    const char *ti,
    const char *ci,
    const char *ii)
    : type_id(ti), class_id(ci), inst_id(ii) {}
};

class TrackedResource {
  const string class_id;
  const string inst_id;

  const tracked_res_t res_id; 
public:
  TrackedResource(
    const char *_type_id,
    const string &_class_id,
    const string &_inst_id)
    : class_id(_class_id), inst_id(_inst_id),
      res_id(_type_id, class_id.c_str(), inst_id.c_str()) {}
  virtual void get_status(Formatter *f) const = 0;
  const tracked_res_t *get_res_id() const {
    return &res_id;
  }

  void log_event(const TrackedOp *op, const string &evt);
  virtual ~TrackedResource() {}
};

#endif
