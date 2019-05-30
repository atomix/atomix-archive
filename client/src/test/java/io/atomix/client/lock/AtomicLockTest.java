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
package io.atomix.client.lock;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.atomix.client.AbstractPrimitiveTest;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Raft lock test.
 */
public class AtomicLockTest extends AbstractPrimitiveTest {

  /**
   * Tests locking and unlocking a lock.
   */
  @Test
  public void testLockUnlock() throws Throwable {
    AtomicLock lock = client().atomicLockBuilder("test-lock-unlock").build();
    long version = lock.lock();
    assertTrue(lock.isLocked());
    assertTrue(lock.isLocked(version));
    assertFalse(lock.isLocked(version + 1));
    lock.unlock();
    assertFalse(lock.isLocked());
    assertFalse(lock.isLocked(version));
  }

  /**
   * Tests releasing a lock when the client's session is closed.
   */
  @Test
  public void testReleaseOnClose() throws Throwable {
    AtomicLock lock1 = client().atomicLockBuilder("test-lock-on-close").build();
    AtomicLock lock2 = client().atomicLockBuilder("test-lock-on-close").build();
    lock1.lock();
    CompletableFuture<Long> future = lock2.async().lock();
    lock1.close();
    future.get(10, TimeUnit.SECONDS);
  }

  /**
   * Tests attempting to acquire a lock.
   */
  @Test
  public void testTryLockFail() throws Throwable {
    AtomicLock lock1 = client().atomicLockBuilder("test-try-lock-fail").build();
    AtomicLock lock2 = client().atomicLockBuilder("test-try-lock-fail").build();

    lock1.lock();

    assertFalse(lock2.tryLock().isPresent());
  }

  /**
   * Tests attempting to acquire a lock.
   */
  @Test
  public void testTryLockSucceed() throws Throwable {
    AtomicLock lock = client().atomicLockBuilder("test-try-lock-succeed").build();
    assertTrue(lock.tryLock().isPresent());
  }

  /**
   * Tests attempting to acquire a lock with a timeout.
   */
  @Test
  public void testTryLockFailWithTimeout() throws Throwable {
    AtomicLock lock1 = client().atomicLockBuilder("test-try-lock-fail-with-timeout").build();
    AtomicLock lock2 = client().atomicLockBuilder("test-try-lock-fail-with-timeout").build();

    lock1.lock();

    assertFalse(lock2.tryLock(Duration.ofSeconds(1)).isPresent());
  }

  /**
   * Tests attempting to acquire a lock with a timeout.
   */
  @Test
  public void testTryLockSucceedWithTimeout() throws Throwable {
    AtomicLock lock1 = client().atomicLockBuilder("test-try-lock-succeed-with-timeout").build();
    AtomicLock lock2 = client().atomicLockBuilder("test-try-lock-succeed-with-timeout").build();

    lock1.lock();

    CompletableFuture<OptionalLong> future = lock2.async().tryLock(Duration.ofSeconds(1));
    lock1.unlock();
    assertTrue(future.get(10, TimeUnit.SECONDS).isPresent());
  }

  /**
   * Tests unlocking a lock with a blocking call in the event thread.
   */
  @Test
  public void testBlockingUnlock() throws Throwable {
    AtomicLock lock1 = client().atomicLockBuilder("test-blocking-unlock").build();
    AtomicLock lock2 = client().atomicLockBuilder("test-blocking-unlock").build();

    lock1.async().lock().thenRun(() -> lock1.unlock());

    lock2.lock();
  }

  /**
   * Tests unlocking another process's lock.
   */
  @Test
  public void testUnlockOtherLock() throws Throwable {
    AtomicLock lock1 = client().atomicLockBuilder("test-blocking-unlock").build();
    AtomicLock lock2 = client().atomicLockBuilder("test-blocking-unlock").build();

    long version = lock1.lock();
    assertTrue(lock2.isLocked());
    assertTrue(lock2.isLocked(version));
    assertTrue(lock2.unlock(version));
    assertFalse(lock1.isLocked());
    assertFalse(lock1.isLocked(version));
    assertFalse(lock2.isLocked());
    assertFalse(lock2.isLocked(version));
  }
}
