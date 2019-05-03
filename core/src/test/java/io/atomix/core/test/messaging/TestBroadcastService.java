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
import java.util.function.Consumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Test broadcast service.
 */
@Component(scope = Component.Scope.TEST)
public class TestBroadcastService implements BroadcastService, Managed {
  @Dependency
  private TestBroadcastSubstrate substrate;

  private final Map<String, Set<Consumer<byte[]>>> listeners = Maps.newConcurrentMap();

  @Override
  public void broadcast(String subject, byte[] message) {
    substrate.get().forEach(service -> {
      Set<Consumer<byte[]>> listeners = service.listeners.get(subject);
      if (listeners != null) {
        listeners.forEach(listener -> {
          listener.accept(message);
        });
      }
    });
  }

  @Override
  public synchronized void addListener(String subject, Consumer<byte[]> listener) {
    listeners.computeIfAbsent(subject, s -> Sets.newCopyOnWriteArraySet()).add(listener);
  }

  @Override
  public synchronized void removeListener(String subject, Consumer<byte[]> listener) {
    Set<Consumer<byte[]>> listeners = this.listeners.get(subject);
    if (listeners != null) {
      listeners.remove(listener);
      if (listeners.isEmpty()) {
        this.listeners.remove(subject);
      }
    }
  }
}
