/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.lock.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import io.atomix.core.lock.AtomicLockType;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Raft atomic value service.
 */
public abstract class AbstractAtomicLockService extends AbstractPrimitiveService<AtomicLockClient> implements AtomicLockService {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(AtomicLockType.instance().namespace())
      .register(LockHolder.class)
      .register(SessionId.class)
      .build());

  LockHolder lock;
  Queue<LockHolder> queue = new ArrayDeque<>();
  final Map<Long, Scheduled> timers = new HashMap<>();

  public AbstractAtomicLockService(PrimitiveType primitiveType) {
    super(primitiveType, AtomicLockClient.class);
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    AtomicLockSnapshot.Builder builder = AtomicLockSnapshot.newBuilder();
    if (lock != null) {
      builder.setLock(LockRequest.newBuilder()
          .setId(lock.id)
          .setIndex(lock.index)
          .setSessionId(lock.session.id())
          .setExpire(lock.expire)
          .build());
    }

    builder.addAllQueue(queue.stream()
        .map(lock -> LockRequest.newBuilder()
            .setId(lock.id)
            .setIndex(lock.index)
            .setSessionId(lock.session.id())
            .setExpire(lock.expire)
            .build())
        .collect(Collectors.toList()));

    builder.build().writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    AtomicLockSnapshot snapshot = AtomicLockSnapshot.parseFrom(input);
    if (snapshot.hasLock()) {
      lock = new LockHolder(
          snapshot.getLock().getId(),
          snapshot.getLock().getIndex(),
          SessionId.from(snapshot.getLock().getSessionId()),
          snapshot.getLock().getExpire());
    }

    queue = snapshot.getQueueList().stream()
        .map(lock -> new LockHolder(
            lock.getId(),
            lock.getIndex(),
            SessionId.from(lock.getSessionId()),
            lock.getExpire()))
        .collect(Collectors.toCollection(ArrayDeque::new));

    // After the snapshot is installed, we need to cancel any existing timers and schedule new ones based on the
    // state provided by the snapshot.
    timers.values().forEach(Scheduled::cancel);
    timers.clear();
    for (LockHolder holder : queue) {
      if (holder.expire > 0) {
        timers.put(holder.index, getScheduler().schedule(Duration.ofMillis(holder.expire - getWallClock().getTime().unixTimestamp()), () -> {
          timers.remove(holder.index);
          queue.remove(holder);
          Session session = getSession(holder.session);
          if (session != null && session.getState().active()) {
            getSession(holder.session).accept(service -> service.failed(holder.id));
          }
        }));
      }
    }
  }

  @Override
  public void onExpire(Session session) {
    releaseSession(session);
  }

  @Override
  public void onClose(Session session) {
    releaseSession(session);
  }

  @Override
  public void lock(int id, long timeout) {
    Session<AtomicLockClient> session = getCurrentSession();
    // If the lock is not already owned, immediately grant the lock to the requester.
    // Note that we still have to publish an event to the session. The event is guaranteed to be received
    // by the client-side primitive after the LOCK response.
    if (lock == null) {
      lock = new LockHolder(
          id,
          getCurrentIndex(),
          session.sessionId(),
          0);
      session.accept(service -> service.locked(id, getCurrentIndex()));
      // If the timeout is 0, that indicates this is a tryLock request. Immediately fail the request.
    } else if (timeout == 0) {
      session.accept(service -> service.failed(id));
      // If a timeout exists, add the request to the queue and set a timer. Note that the lock request expiration
      // time is based on the *state machine* time - not the system time - to ensure consistency across servers.
    } else if (timeout > 0) {
      LockHolder holder = new LockHolder(
          id,
          getCurrentIndex(),
          session.sessionId(),
          getWallClock().getTime().unixTimestamp() + timeout);
      queue.add(holder);
      timers.put(getCurrentIndex(), getScheduler().schedule(Duration.ofMillis(timeout), () -> {
        // When the lock request timer expires, remove the request from the queue and publish a FAILED
        // event to the session. Note that this timer is guaranteed to be executed in the same thread as the
        // state machine commands, so there's no need to use a lock here.
        timers.remove(getCurrentIndex());
        queue.remove(holder);
        if (session.getState().active()) {
          session.accept(service -> service.failed(id));
        }
      }));
      // If the lock is -1, just add the request to the queue with no expiration.
    } else {
      LockHolder holder = new LockHolder(
          id,
          getCurrentIndex(),
          session.sessionId(),
          0);
      queue.add(holder);
    }
  }

  @Override
  public void unlock(int id) {
    if (lock != null) {
      // If the commit's session does not match the current lock holder, preserve the existing lock.
      // If the current lock ID does not match the requested lock ID, preserve the existing lock.
      // However, ensure the associated lock request is removed from the queue.
      if (!lock.session.equals(getCurrentSession().sessionId()) || lock.id != id) {
        Iterator<LockHolder> iterator = queue.iterator();
        while (iterator.hasNext()) {
          LockHolder lock = iterator.next();
          if (lock.session.equals(getCurrentSession().sessionId()) && lock.id == id) {
            iterator.remove();
            Scheduled timer = timers.remove(lock.index);
            if (timer != null) {
              timer.cancel();
            }
          }
        }
        return;
      }

      // The lock has been released. Populate the lock from the queue.
      lock = queue.poll();
      while (lock != null) {
        // If the waiter has a lock timer, cancel the timer.
        Scheduled timer = timers.remove(lock.index);
        if (timer != null) {
          timer.cancel();
        }

        // Notify the client that it has acquired the lock.
        Session lockSession = getSession(lock.session);
        if (lockSession != null && lockSession.getState().active()) {
          getSession(lock.session).accept(service -> service.locked(lock.id, getCurrentIndex()));
          break;
        }
        lock = queue.poll();
      }
    }
  }

  @Override
  public boolean unlock(long index) {
    // If the lock is not currently held, just return false.
    if (lock == null) {
      return false;
    }

    // If the index does not match the current lock index, reject the request.
    if (index > 0 && lock.index != index) {
      return false;
    }

    // Notify the lock holder that it has lost the lock.
    getSession(lock.session).accept(service -> service.unlocked(lock.id, getCurrentIndex()));

    // The lock has been released. Populate the lock from the queue.
    lock = queue.poll();
    while (lock != null) {
      // If the waiter has a lock timer, cancel the timer.
      Scheduled timer = timers.remove(lock.index);
      if (timer != null) {
        timer.cancel();
      }

      // Notify the client that it has acquired the lock.
      Session lockSession = getSession(lock.session);
      if (lockSession != null && lockSession.getState().active()) {
        getSession(lock.session).accept(service -> service.locked(lock.id, getCurrentIndex()));
        break;
      }
      lock = queue.poll();
    }
    return true;
  }

  @Override
  public boolean isLocked(long index) {
    return lock != null && (index == 0 || lock.index == index);
  }

  /**
   * Handles a session that has been closed by a client or expired by the cluster.
   * <p>
   * When a session is removed, if the session is the current lock holder then the lock is released and the next
   * session waiting in the queue is granted the lock. Additionally, all pending lock requests for the session
   * are removed from the lock queue.
   *
   * @param session the closed session
   */
  private void releaseSession(Session session) {
    // Remove all instances of the session from the lock queue.
    queue.removeIf(lock -> lock.session.equals(session.sessionId()));

    // If the removed session is the current holder of the lock, nullify the lock and attempt to grant it
    // to the next waiter in the queue.
    if (lock != null && lock.session.equals(session.sessionId())) {
      lock = queue.poll();
      while (lock != null) {
        // If the waiter has a lock timer, cancel the timer.
        Scheduled timer = timers.remove(lock.index);
        if (timer != null) {
          timer.cancel();
        }

        // Notify the client that it has acquired the lock.
        Session lockSession = getSession(lock.session);
        if (lockSession != null && lockSession.getState().active()) {
          getSession(lock.session).accept(service -> service.locked(lock.id, lock.index));
          break;
        }
        lock = queue.poll();
      }
    }
  }

  class LockHolder {
    final int id;
    final long index;
    final SessionId session;
    final long expire;

    LockHolder(int id, long index, SessionId session, long expire) {
      this.id = id;
      this.index = index;
      this.session = session;
      this.expire = expire;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("id", id)
          .add("index", index)
          .add("session", session)
          .add("expire", expire)
          .toString();
    }
  }
}
