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
package io.atomix.primitive.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.service.ServiceTypeRegistry;
import io.atomix.utils.ServiceException;
import io.atomix.utils.component.Cardinality;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Service type registry.
 */
@Component
public class ServiceTypeRegistryImpl implements ServiceTypeRegistry, Managed {
  @Dependency(value = ServiceType.class, cardinality = Cardinality.MULTIPLE)
  private List<ServiceType> types;

  private final Map<String, ServiceType> typeMap = new ConcurrentHashMap<>();

  @Override
  public Collection<ServiceType> getServiceTypes() {
    return types;
  }

  @Override
  public ServiceType getServiceType(String typeName) {
    ServiceType type = typeMap.get(typeName);
    if (type == null) {
      throw new ServiceException("Unknown service type " + typeName);
    }
    return type;
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
