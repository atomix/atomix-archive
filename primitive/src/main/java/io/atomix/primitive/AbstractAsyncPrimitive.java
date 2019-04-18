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
package io.atomix.primitive;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for asynchronous primitives.
 */
public abstract class AbstractAsyncPrimitive<T extends AbstractPrimitiveProxy> implements AsyncPrimitive {
  private final T proxy;

  public AbstractAsyncPrimitive(T proxy) {
    this.proxy = proxy;
  }

  /**
   * Returns the primitive proxy.
   *
   * @return the primitive proxy
   */
  protected T getProxy() {
    return proxy;
  }

  @Override
  public String name() {
    return proxy.name();
  }

  @Override
  public PrimitiveType type() {
    return proxy.type();
  }

  @Override
  public CompletableFuture<Void> close() {
    return proxy.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return proxy.delete();
  }
}