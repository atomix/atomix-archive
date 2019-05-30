/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.client.map;

import io.atomix.api.headers.Name;
import io.atomix.client.PrimitiveManagementService;

/**
 * Builder for {@link DistributedMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class DistributedMapBuilder<K, V>
    extends MapBuilder<DistributedMapBuilder<K, V>, DistributedMap<K, V>, K, V> {
  protected DistributedMapBuilder(Name name, PrimitiveManagementService managementService) {
    super(name, managementService);
  }
}
