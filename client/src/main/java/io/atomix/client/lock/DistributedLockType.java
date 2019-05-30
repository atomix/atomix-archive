/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.client.lock;

import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.PrimitiveType;
import io.atomix.client.lock.impl.DefaultDistributedLockBuilder;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed lock primitive type.
 */
public class DistributedLockType implements PrimitiveType<DistributedLockBuilder, DistributedLock> {
  private static final DistributedLockType INSTANCE = new DistributedLockType();

  /**
   * Returns a new distributed lock type.
   *
   * @return a new distributed lock type
   */
  public static DistributedLockType instance() {
    return INSTANCE;
  }

  @Override
  public DistributedLockBuilder newBuilder(PrimitiveId id, PrimitiveManagementService managementService) {
    return new DefaultDistributedLockBuilder(id, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
