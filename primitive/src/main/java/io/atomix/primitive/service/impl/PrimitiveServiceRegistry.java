/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.primitive.service.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Primitive service registry.
 */
public class PrimitiveServiceRegistry implements Iterable<PrimitiveServiceContext> {
  private final Map<String, PrimitiveServiceContext> services = new ConcurrentHashMap<>();

  /**
   * Registers a new service.
   *
   * @param service the service to register
   */
  public void registerService(PrimitiveServiceContext service) {
    services.put(service.serviceName(), service);
  }

  /**
   * Unregisters the given service.
   *
   * @param service the service to unregister
   */
  public void unregisterService(PrimitiveServiceContext service) {
    services.remove(service.serviceName());
  }

  /**
   * Gets a registered service by name.
   *
   * @param name the service name
   * @return the registered service
   */
  public PrimitiveServiceContext getService(String name) {
    return services.get(name);
  }

  @Override
  public Iterator<PrimitiveServiceContext> iterator() {
    return services.values().iterator();
  }
}
