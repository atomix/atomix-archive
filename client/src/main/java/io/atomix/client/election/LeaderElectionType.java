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
package io.atomix.client.election;

import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.PrimitiveType;
import io.atomix.client.election.impl.DefaultLeaderElectionBuilder;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Leader elector primitive type.
 */
public class LeaderElectionType<T> implements PrimitiveType<LeaderElectionBuilder<T>, LeaderElection<T>> {
  private static final LeaderElectionType INSTANCE = new LeaderElectionType();

  /**
   * Returns a new leader elector type.
   *
   * @param <T> the election candidate type
   * @return a new leader elector type
   */
  @SuppressWarnings("unchecked")
  public static <T> LeaderElectionType<T> instance() {
    return INSTANCE;
  }

  @Override
  public LeaderElectionBuilder<T> newBuilder(PrimitiveId id, PrimitiveManagementService managementService) {
    return new DefaultLeaderElectionBuilder<>(id, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}