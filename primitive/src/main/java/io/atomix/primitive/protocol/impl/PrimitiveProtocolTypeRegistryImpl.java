/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.protocol.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.utils.component.Cardinality;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Primitive protocol type registry.
 */
@Component
public class PrimitiveProtocolTypeRegistryImpl implements PrimitiveProtocolTypeRegistry, Managed {
  @Dependency(value = PrimitiveProtocol.Type.class, cardinality = Cardinality.MULTIPLE)
  private List<PrimitiveProtocol.Type> types;

  private final Map<String, PrimitiveProtocol.Type> typeMap = new ConcurrentHashMap<>();

  @Override
  public Collection<PrimitiveProtocol.Type> getProtocolTypes() {
    return types;
  }

  @Override
  public PrimitiveProtocol.Type getProtocolType(String type) {
    return typeMap.get(type);
  }

  @Override
  public CompletableFuture<Void> start() {
    types.forEach(type -> typeMap.put(type.name(), type));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }
}
