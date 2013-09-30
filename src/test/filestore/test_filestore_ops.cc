// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <boost/scoped_ptr.hpp>
#include "os/FileStore.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "os/LevelDBStore.h"
#include "os/KeyValueDB.h"
#include "os/ObjectStore.h"

void usage(const string &name) {
  std::cerr << "Usage: " << name << " store_path store_journal db_path"
	    << std::endl;
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  std::cerr << "args: " << args << std::endl;
  if (args.size() < 3) {
    usage(argv[0]);
    return 1;
  }

  string store_path(args[0]);
  string store_dev(args[1]);
  string db_path(args[2]);

  LevelDBStore *_db = new LevelDBStore(g_ceph_context, db_path);
  assert(!_db->create_and_open(std::cerr));
  boost::scoped_ptr<KeyValueDB> db(_db);
  boost::scoped_ptr<ObjectStore> store(new FileStore(store_path, store_dev));


  std::cerr << "mkfs" << std::endl;
  assert(!store->mkfs());
  ObjectStore::Transaction t;
  assert(!store->mount());
  coll_t test_coll("test_coll");
  t.create_collection(test_coll);
  store->apply_transaction(t);

  std::cerr << "creating empty file" << std::endl;

  ObjectStore::Transaction empty_create_t;
  bufferlist empty_bl;
  hobject_t zero_oid(sobject_t("zero_write", CEPH_NOSNAP));
  empty_create_t.write(test_coll, zero_oid, 0, 0, empty_bl);
  int r = store->apply_transaction(empty_create_t);
  assert(r == 0);

  std::cerr << "writing empty to empty file" << std::endl;

  ObjectStore::Transaction empty_write_t;
  empty_write_t.write(test_coll, zero_oid, 0, 0, empty_bl);
  r = store->apply_transaction(empty_write_t);
  assert(r == 0);

  std::cerr << "exiting" << std::endl;
  store->umount();  

  return 0;
}
