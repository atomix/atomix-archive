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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.service.ServiceTypeRegistry;
import io.atomix.utils.ServiceException;

/**
 * Service type registry.
 */
public class DefaultServiceTypeRegistry implements ServiceTypeRegistry {
  private final Map<String, ServiceType> serviceTypes = new ConcurrentHashMap<>();

  public DefaultServiceTypeRegistry(Collection<ServiceType> serviceTypes) {
    serviceTypes.forEach(type -> this.serviceTypes.put(type.name(), type));
  }

  @Override
  public Collection<ServiceType> getServiceTypes() {
    return serviceTypes.values();
  }

  @Override
  public ServiceType getServiceType(String typeName) {
    ServiceType type = serviceTypes.get(typeName);
    if (type == null) {
      throw new ServiceException("Unknown service type " + typeName);
    }
    return type;
  }
}
