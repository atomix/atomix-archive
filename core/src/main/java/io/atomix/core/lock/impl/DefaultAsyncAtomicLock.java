/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.lock.impl;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.Maps;
import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AtomicLock;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.time.Version;

/**
 * Raft lock.
 */
public class DefaultAsyncAtomicLock extends AbstractAsyncPrimitive<LockProxy> implements AsyncAtomicLock {
  private final Map<Integer, LockAttempt> attempts = Maps.newConcurrentMap();
  private final AtomicInteger id = new AtomicInteger();
  private final AtomicInteger lock = new AtomicInteger();

  public DefaultAsyncAtomicLock(LockProxy proxy) {
    super(proxy);
    proxy.onStateChange(this::onStateChange);
    proxy.onLock(this::onLock);
  }

  private void onStateChange(PrimitiveState state) {
    if (state != PrimitiveState.CONNECTED) {
      for (LockAttempt attempt : attempts.values()) {
        getProxy().unlock(UnlockRequest.newBuilder()
            .setId(attempt.id())
            .build());
        attempt.completeExceptionally(new PrimitiveException.Unavailable());
      }
    }
  }

  private void onLock(LockEvent event) {
    // Remove the LockAttempt from the attempts map and complete it with the lock version if it exists.
    // If the attempt no longer exists, it likely was expired by a client-side timer.
    if (event.getAcquired()) {
      LockAttempt attempt = attempts.remove(event.getId());
      if (attempt != null) {
        attempt.complete(new Version(event.getMetadata().getIndex()));
      } else {
        getProxy().unlock(UnlockRequest.newBuilder()
            .setId(event.getId())
            .build());
      }
    } else if (!lock.compareAndSet(event.getId(), 0)) {
      // Remove the LockAttempt from the attempts map and complete it with a null value if it exists.
      // If the attempt no longer exists, it likely was expired by a client-side timer.
      LockAttempt attempt = attempts.remove(event.getId());
      if (attempt != null) {
        attempt.complete(null);
      }
    }
  }

  @Override
  public CompletableFuture<Version> lock() {
    // Create and register a new attempt and invoke the LOCK operation on the replicated state machine.
    LockAttempt attempt = new LockAttempt();
    getProxy().lock(LockRequest.newBuilder()
        .setId(attempt.id())
        .setTimeout(-1)
        .build()).whenComplete((response, error) -> {
      if (error != null) {
        attempt.completeExceptionally(error);
      }
    });
    return attempt;
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock() {
    // If the proxy is currently disconnected from the cluster, we can just fail the lock attempt here.
    PrimitiveState state = getProxy().getState();
    if (state != PrimitiveState.CONNECTED) {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    // Create and register a new attempt and invoke the LOCK operation on teh replicated state machine with
    // a 0 timeout. The timeout will cause the state machine to immediately reject the request if the lock is
    // already owned by another process.
    LockAttempt attempt = new LockAttempt();
    getProxy().lock(LockRequest.newBuilder()
        .setId(attempt.id())
        .setTimeout(0)
        .build()).whenComplete((response, error) -> {
      if (error != null) {
        attempt.completeExceptionally(error);
      }
    });
    return attempt.thenApply(Optional::ofNullable);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock(Duration timeout) {
    // Create a lock attempt with a client-side timeout and fail the lock if the timer expires.
    // Because time does not progress at the same rate on different nodes, we can't guarantee that
    // the lock won't be granted to this process after it's expired here. Thus, if this timer expires and
    // we fail the lock on the client, we also still need to send an UNLOCK command to the cluster in case it's
    // later granted by the cluster. Note that the semantics of the Raft client will guarantee this operation
    // occurs after any prior LOCK attempt, and the Raft client will retry the UNLOCK request until successful.
    // Additionally, sending the unique lock ID with the command ensures we won't accidentally unlock a different
    // lock call also granted to this process.
    LockAttempt attempt = new LockAttempt(timeout, a -> {
      a.complete(null);
      getProxy().unlock(UnlockRequest.newBuilder()
          .setId(a.id())
          .build());
    });

    // Invoke the LOCK operation on the replicated state machine with the given timeout. If the lock is currently
    // held by another process, the state machine will add the attempt to a queue and publish a FAILED event if
    // the timer expires before this process can be granted the lock. If the client cannot reach the Raft cluster,
    // the client-side timer will expire the attempt.
    getProxy().lock(LockRequest.newBuilder()
        .setId(attempt.id())
        .setTimeout(timeout.toMillis())
        .build())
        .whenComplete((response, error) -> {
          if (error != null) {
            attempt.completeExceptionally(error);
          }
        });
    return attempt.thenApply(Optional::ofNullable);
  }

  @Override
  public CompletableFuture<Void> unlock() {
    // Use the current lock ID to ensure we only unlock the lock currently held by this process.
    int lock = this.lock.getAndSet(0);
    if (lock != 0) {
      return getProxy().unlock(UnlockRequest.newBuilder()
          .setId(lock)
          .build())
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> unlock(Version version) {
    return getProxy().unlock(UnlockRequest.newBuilder()
        .setIndex(version.value())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> isLocked() {
    return isLocked(new Version(0));
  }

  @Override
  public CompletableFuture<Boolean> isLocked(Version version) {
    return getProxy().isLocked(IsLockedRequest.newBuilder()
        .setIndex(version.value())
        .build())
        .thenApply(response -> response.getLocked());
  }

  @Override
  public AtomicLock sync(Duration operationTimeout) {
    return new BlockingAtomicLock(this, operationTimeout.toMillis());
  }

  /**
   * Lock attempt.
   */
  private class LockAttempt extends CompletableFuture<Version> {
    private final int id;
    private final Scheduled scheduled;

    LockAttempt() {
      this(null, null);
    }

    LockAttempt(Duration duration, Consumer<LockAttempt> callback) {
      this.id = DefaultAsyncAtomicLock.this.id.incrementAndGet();
      this.scheduled = duration != null && callback != null
          ? getProxy().context().schedule(duration, () -> callback.accept(this)) : null;
      attempts.put(id, this);
    }

    /**
     * Returns the lock attempt ID.
     *
     * @return the lock attempt ID
     */
    int id() {
      return id;
    }

    @Override
    public boolean complete(Version version) {
      if (isDone()) {
        return super.complete(null);
      }
      cancel();
      if (version != null) {
        lock.set(id);
        return super.complete(version);
      } else {
        return super.complete(null);
      }
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
      cancel();
      return super.completeExceptionally(ex);
    }

    private void cancel() {
      if (scheduled != null) {
        scheduled.cancel();
      }
      attempts.remove(id);
    }
  }
}