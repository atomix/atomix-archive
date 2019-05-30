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
package io.atomix.client.cache;

import io.atomix.api.headers.Name;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.SyncPrimitive;
import io.atomix.client.impl.ManagedPrimitiveBuilder;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Cached distributed primitive builder.
 */
public abstract class CachedPrimitiveBuilder<B extends CachedPrimitiveBuilder<B, P>, P extends SyncPrimitive>
    extends ManagedPrimitiveBuilder<B, P> {
  private static final int DEFAULT_CACHE_SIZE = 1000;

  protected boolean cacheEnabled = false;
  protected int cacheSize = DEFAULT_CACHE_SIZE;

  protected CachedPrimitiveBuilder(Name name, PrimitiveManagementService managementService) {
    super(name, managementService);
  }

  /**
   * Enables caching for the primitive.
   *
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withCacheEnabled() {
    return withCacheEnabled(true);
  }

  /**
   * Sets whether caching is enabled.
   *
   * @param cacheEnabled whether caching is enabled
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withCacheEnabled(boolean cacheEnabled) {
    this.cacheEnabled = cacheEnabled;
    return (B) this;
  }

  /**
   * Sets the cache size.
   *
   * @param cacheSize the cache size
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withCacheSize(int cacheSize) {
    checkArgument(cacheSize > 0, "cacheSize must be positive");
    this.cacheSize = cacheSize;
    return (B) this;
  }
}
