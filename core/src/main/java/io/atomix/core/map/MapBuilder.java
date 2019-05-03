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
package io.atomix.core.map;

import io.atomix.core.cache.CachedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;

/**
 * Base map builder.
 */
public abstract class MapBuilder<B extends MapBuilder<B, C, P, K, V>, C extends MapConfig<C>, P extends SyncPrimitive, K, V>
    extends CachedPrimitiveBuilder<B, C, P> {
  protected MapBuilder(PrimitiveType type, String name, C config, PrimitiveManagementService managementService) {
    super(type, name, config, managementService);
  }
}
