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
package io.atomix.client.map;

import io.atomix.api.headers.Name;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.SyncPrimitive;
import io.atomix.client.cache.CachedPrimitiveBuilder;

/**
 * Base map builder.
 */
public abstract class MapBuilder<B extends MapBuilder<B, P, K, V>, P extends SyncPrimitive, K, V>
    extends CachedPrimitiveBuilder<B, P> {
  protected MapBuilder(Name name, PrimitiveManagementService managementService) {
    super(name, managementService);
  }
}
