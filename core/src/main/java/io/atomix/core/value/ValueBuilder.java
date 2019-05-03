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
package io.atomix.core.value;

import io.atomix.primitive.ManagedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;

/**
 * Builder for constructing new value primitive instances.
 *
 * @param <V> value type
 */
public abstract class ValueBuilder<B extends ValueBuilder<B, C, P, V>, C extends ValueConfig<C>, P extends SyncPrimitive, V>
    extends ManagedPrimitiveBuilder<B, C, P> {
  protected ValueBuilder(PrimitiveType primitiveType, String name, C config, PrimitiveManagementService managementService) {
    super(primitiveType, name, config, managementService);
  }
}
