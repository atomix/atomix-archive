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
package io.atomix.primitive.partition.impl;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.service.ServiceTypeRegistry;
import io.atomix.primitive.session.SessionProtocolService;

/**
 * Default partition management service.
 */
public class DefaultPartitionManagementService implements PartitionManagementService {
  private final ClusterMembershipService membershipService;
  private final ClusterCommunicationService communicationService;
  private final ServiceTypeRegistry serviceTypes;
  private final PrimaryElectionService electionService;
  private final SessionProtocolService protocolService;

  public DefaultPartitionManagementService(
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      ServiceTypeRegistry serviceTypes,
      PrimaryElectionService electionService,
      SessionProtocolService protocolService) {
    this.membershipService = membershipService;
    this.communicationService = communicationService;
    this.serviceTypes = serviceTypes;
    this.electionService = electionService;
    this.protocolService = protocolService;
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  @Override
  public ClusterCommunicationService getMessagingService() {
    return communicationService;
  }

  @Override
  public ServiceTypeRegistry getServiceTypes() {
    return serviceTypes;
  }

  @Override
  public PrimaryElectionService getElectionService() {
    return electionService;
  }

  @Override
  public SessionProtocolService getSessionProtocolService() {
    return protocolService;
  }
}
