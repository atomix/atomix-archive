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
package io.atomix.core.barrier.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.barrier.DistributedCyclicBarrierType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

/**
 * Default cyclic barrier service.
 */
public class DefaultDistributedCyclicBarrierService extends AbstractPrimitiveService<DistributedCyclicBarrierClient> implements DistributedCyclicBarrierService {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(DistributedCyclicBarrierType.instance().namespace())
      .register(SessionId.class)
      .build());

  private Set<SessionId> parties = Sets.newHashSet();
  private long barrierId;
  private Map<SessionId, Waiter> waiters = Maps.newLinkedHashMap();
  private boolean broken;

  public DefaultDistributedCyclicBarrierService() {
    super(DistributedCyclicBarrierType.instance(), DistributedCyclicBarrierClient.class);
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    DistributedCyclicBarrierSnapshot.newBuilder()
        .addAllParties(parties.stream()
            .map(SessionId::id)
            .collect(Collectors.toList()))
        .setBarrierId(barrierId)
        .setBroken(broken)
        .putAllWaiters(waiters.entrySet().stream()
            .map(e -> Maps.immutableEntry(e.getKey().id(), e.getValue().timeout))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    DistributedCyclicBarrierSnapshot snapshot = DistributedCyclicBarrierSnapshot.parseFrom(input);
    parties = snapshot.getPartiesList().stream()
        .map(SessionId::from)
        .collect(Collectors.toCollection(HashSet::new));
    barrierId = snapshot.getBarrierId();
    broken = snapshot.getBroken();
    waiters = snapshot.getWaitersMap().entrySet().stream()
        .map(e -> Maps.immutableEntry(SessionId.from(e.getKey()), new Waiter(
            e.getValue(), getScheduler().schedule(Duration.ofMillis(e.getValue() - getWallClock().getTime().unixTimestamp()), () -> timeout(barrierId)))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
  }

  @Override
  public void onExpire(Session session) {
    onClose(session);
  }

  @Override
  public void onClose(Session session) {
    parties.remove(session.sessionId());

    Waiter waiter = waiters.remove(session.sessionId());
    if (waiter != null) {
      waiter.cancel();

      if (waiters.isEmpty()) {
        barrierId = 0;
        broken = false;
      } else if (waiters.size() == getParties()) {
        AtomicInteger index = new AtomicInteger(waiters.size());
        AtomicReference<SessionId> last = new AtomicReference<>();
        waiters.keySet().forEach(sessionId -> {
          getSession(sessionId).accept(client -> client.release(barrierId, index.decrementAndGet()));
          last.set(sessionId);
        });
        getSession(last.get()).accept(client -> client.runAction());
        waiters.clear();
        barrierId = 0;
        broken = false;
      }
    }
  }

  /**
   * Times out the given barrier instance.
   *
   * @param barrierId the barrier ID to time out
   */
  private void timeout(long barrierId) {
    if (this.barrierId == barrierId && !broken) {
      broken = true;
      parties.forEach(session -> getSession(session).accept(client -> client.broken(barrierId)));
    }
  }

  @Override
  public void join() {
    parties.add(getCurrentSession().sessionId());
  }

  @Override
  public CyclicBarrierResult<Long> await(long timeout) {
    if (barrierId == 0) {
      barrierId = getCurrentIndex();
    }

    if (broken) {
      return new CyclicBarrierResult<>(CyclicBarrierResult.Status.BROKEN, barrierId);
    }

    SessionId sessionId = getCurrentSession().sessionId();
    if (timeout > 0) {
      waiters.put(sessionId, new Waiter(
          getWallClock().getTime().unixTimestamp() + timeout,
          getScheduler().schedule(Duration.ofMillis(timeout), () -> timeout(barrierId))));
    } else {
      waiters.put(sessionId, new Waiter(0, null));
    }

    if (waiters.size() == getParties()) {
      AtomicInteger index = new AtomicInteger(waiters.size());
      waiters.keySet().forEach(session -> getSession(session).accept(client -> client.release(barrierId, index.decrementAndGet())));
      getCurrentSession().accept(client -> client.runAction());
      waiters.clear();
    }
    return new CyclicBarrierResult<>(CyclicBarrierResult.Status.OK, barrierId);
  }

  @Override
  public int getNumberWaiting() {
    return waiters.size();
  }

  @Override
  public int getParties() {
    return parties.size();
  }

  @Override
  public boolean isBroken(long barrierId) {
    return (barrierId == 0 || this.barrierId == barrierId) && broken;
  }

  @Override
  public void reset(long barrierId) {
    if (this.barrierId > 0 && (barrierId == 0 || this.barrierId == barrierId)) {
      waiters.forEach((sessionId, scheduled) -> {
        if (scheduled != null) {
          scheduled.cancel();
        }
        getSession(sessionId).accept(client -> client.broken(this.barrierId));
      });
      waiters.clear();
      broken = false;
      this.barrierId = 0;
    }
  }

  private static class Waiter {
    private final long timeout;
    private final Scheduled timer;

    Waiter(long timeout, Scheduled timer) {
      this.timeout = timeout;
      this.timer = timer;
    }

    void cancel() {
      if (timer != null) {
        timer.cancel();
      }
    }
  }
}
