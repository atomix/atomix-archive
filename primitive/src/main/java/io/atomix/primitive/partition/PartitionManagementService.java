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
package io.atomix.primitive.partition;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.service.ServiceTypeRegistry;

/**
 * Partition management service.
 */
public interface PartitionManagementService {

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  ClusterMembershipService getMembershipService();

  /**
   * Returns the service type registry.
   *
   * @return the service type registry
   */
  ServiceTypeRegistry getServiceTypes();

  /**
   * Returns the partition metadata service.
   *
   * @return the partition metadata service
   */
  PartitionMetadataService getMetadataService();

  /**
   * Returns the primary election service.
   *
   * @return the primary election service
   */
  PrimaryElectionService getElectionService();

  /**
   * Returns the partition group membership service.
   *
   * @return the partition group membership service
   */
  PartitionGroupMembershipService getGroupMembershipService();

}
