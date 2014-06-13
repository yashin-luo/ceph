// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef NOTIFYINGLOCK_H_
#define NOTIFYINGLOCK_H_

#include "RWLock.h"

class NotifyingLock {
  /**
   * We want a RWLock which allows us to:
   * 1) optionally go on a waitlist if we fail to get the read lock,
   *    rather than blocking indefinitely.
   * 2) Synchronously notify everybody on the waitlist once the lock
   *    becomes available.
   *
   * To accomplish this, we have a RWLock "wrapped_lock", a list
   * notifiers, and a Mutex notifiers_lock. Normal get_read/get_write
   * calls simply pass through to the wrapped_lock. A try_get_read()
   * call that fails wrapped_lock.try_get_read() will grab the
   * notifiers_lock and add itself to the notifiers list (after trying
   * to get the read lock again, to handle races).
   * put_write() will always grab the notifiers_lock, call notify()
   * on each notifier, and then drop the write lock.
   * The clever reader will notice we don't have a strict lock
   * ordering in that algorithm, but it won't deadlock (since we don't
   * block on the reader side) so we just disable lockdep on notifiers_lock.
   *
   * Notice that try_read_lock() CAN block if a dropped write lock
   * is triggering notifiers, but the only way it can propagate out
   * as a deadlock (instead of just a slow lock acquisition) to user code
   * is if the passed-in Notifier blocks (by, for instance, attempting to
   * make use of the NotifyingLock again...).
   */
public:
  class Notifier {
  public:
    virtual void notify() = 0;
  protected:
    virtual ~Notifier() {};
  };

  class RLocker {
    const NotifyingLock& m_lock;
  public:
    RLocker(const NotifyingLock& lock) : m_lock(lock) {
      m_lock.get_read();
    }
    ~RLocker() { m_lock.put_read(); }
  };

  class WLocker {
    NotifyingLock& m_lock;
  public:
    WLocker(NotifyingLock& lock) : m_lock(lock) {
      m_lock.get_write();
    }
    ~WLocker() { m_lock.put_write(); }
  };
private:
  std::string notifiers_name;
  std::string wrapped_name;
  Mutex notifiers_lock;
  RWLock wrapped_lock;
  std::list<Notifier*> notifiers;
public:
  NotifyingLock(const char *n) : notifiers_name(n),
  wrapped_name(notifiers_name.append("::notifiers_lock").substr(0, strlen(n))),
  notifiers_lock(notifiers_name.c_str(), false),
  wrapped_lock(wrapped_name.c_str()) {}

  // deliberately disable copy constructor and equals operator
  NotifyingLock(const NotifyingLock& other);
  const NotifyingLock& operator=(const NotifyingLock& other);

  // pass-thru to RWLock functions
  void get_read() const {
    wrapped_lock.get_read();
  }
  void put_read() const { wrapped_lock.put_read(); }
  void get_write() {
    wrapped_lock.get_write();
  }
  void put_write() {
    Mutex::Locker l(notifiers_lock);
    for (std::list<Notifier*>::iterator i = notifiers.begin();
        i != notifiers.end();
        ++i) {
      (*i)->notify();
    }
    notifiers.clear();
    wrapped_lock.put_write();
  }

  bool try_get_read(Notifier *notify) {
    if (wrapped_lock.try_get_read()) {
      return true; // we succeeded!
    }

    Mutex::Locker l(notifiers_lock); // handle lockdep issue!
    if (wrapped_lock.try_get_read()) {
      return true;
    }
    notifiers.push_back(notify);
    return false;
  }
};

#endif /* NOTIFYINGLOCK_H_ */
