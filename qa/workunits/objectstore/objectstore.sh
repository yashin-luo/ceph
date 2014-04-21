#!/bin/bash
#
# Copyright (C) 2014 Inktank
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
set -e

: ${EXTRA_ARGS:=}

trap "rm -fr store_test_temp_dir && rm store_test_temp_journal" EXIT QUIT INT

ceph_test_objectstore --gtest_filter=ObjectStore/StoreTest.* $EXTRA_ARGS

echo OK
