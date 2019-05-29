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
package io.atomix.service.proxy;

import java.util.concurrent.CompletableFuture;

import io.atomix.service.client.ServiceClient;

/**
 * Base class for primitive proxies.
 */
public abstract class SimpleServiceProxy extends AbstractServiceProxy<ServiceClient> {
  protected SimpleServiceProxy(ServiceClient client) {
    super(client);
  }

  @Override
  public String name() {
    return getClient().name();
  }

  @Override
  public String type() {
    return getClient().type();
  }

  /**
   * Creates a service.
   *
   * @return a future to be completed once the service has been created
   */
  public CompletableFuture<Void> create() {
    return getClient().create();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return getClient().delete();
  }
}