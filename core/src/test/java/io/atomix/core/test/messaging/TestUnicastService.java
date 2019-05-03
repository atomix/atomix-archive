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
package io.atomix.core.test.messaging;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.messaging.UnicastService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.net.Address;

/**
 * Test unicast service.
 */
@Component(scope = Component.Scope.TEST)
public class TestUnicastService implements UnicastService, Managed {
  @Dependency
  private TestUnicastSubstrate substrate;

  @Dependency
  private MemberService memberService;

  private Address address;
  private final Map<String, Map<BiConsumer<Address, byte[]>, Executor>> listeners = Maps.newConcurrentMap();
  private final Set<Address> partitions = Sets.newConcurrentHashSet();

  @Override
  public CompletableFuture start() {
    this.address = memberService.getLocalMember().address();
    substrate.register(address, this);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Returns the service address.
   *
   * @return the service address
   */
  Address address() {
    return address;
  }

  /**
   * Partitions the node from the given address.
   */
  void partition(Address address) {
    partitions.add(address);
  }

  /**
   * Heals the partition from the given address.
   */
  void heal(Address address) {
    partitions.remove(address);
  }

  /**
   * Returns a boolean indicating whether this node is partitioned from the given address.
   *
   * @param address the address to check
   * @return whether this node is partitioned from the given address
   */
  boolean isPartitioned(Address address) {
    return partitions.contains(address);
  }

  @Override
  public void unicast(Address address, String subject, byte[] message) {
    if (isPartitioned(address)) {
      return;
    }

    TestUnicastService service = substrate.get(address);
    if (service != null) {
      Map<BiConsumer<Address, byte[]>, Executor> listeners = service.listeners.get(subject);
      if (listeners != null) {
        listeners.forEach((listener, executor) -> executor.execute(() -> listener.accept(this.address, message)));
      }
    }
  }

  @Override
  public synchronized void addListener(String subject, BiConsumer<Address, byte[]> listener, Executor executor) {
    listeners.computeIfAbsent(subject, s -> Maps.newConcurrentMap()).put(listener, executor);
  }

  @Override
  public synchronized void removeListener(String subject, BiConsumer<Address, byte[]> listener) {
    Map<BiConsumer<Address, byte[]>, Executor> listeners = this.listeners.get(subject);
    if (listeners != null) {
      listeners.remove(listener);
      if (listeners.isEmpty()) {
        this.listeners.remove(subject);
      }
    }
  }
}
