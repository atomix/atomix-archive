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
package io.atomix.core.impl;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.PrimitiveSessionIdManager;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadService;

/**
 * Default primitive management service.
 */
@Component
public class PrimitiveManagementManager implements PrimitiveManagementService {

  @Dependency
  private ClusterMembershipService membershipService;

  @Dependency
  private ClusterCommunicationService communicationService;

  @Dependency
  private ClusterEventService eventService;

  @Dependency
  private PartitionService partitionService;

  @Dependency
  private PrimitiveCache primitiveCache;

  @Dependency
  private PrimitiveRegistry primitiveRegistry;

  @Dependency
  private PrimitiveTypeRegistry primitiveTypeRegistry;

  @Dependency
  private PrimitiveProtocolTypeRegistry protocolTypeRegistry;

  @Dependency
  private PartitionGroupTypeRegistry partitionGroupTypeRegistry;

  @Dependency
  private PrimitiveSessionIdManager sessionIdService;

  @Dependency
  private ThreadService threadService;

  @Override
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  @Override
  public ClusterCommunicationService getCommunicationService() {
    return communicationService;
  }

  @Override
  public ClusterEventService getEventService() {
    return eventService;
  }

  @Override
  public PartitionService getPartitionService() {
    return partitionService;
  }

  @Override
  public PrimitiveCache getPrimitiveCache() {
    return primitiveCache;
  }

  @Override
  public PrimitiveRegistry getPrimitiveRegistry() {
    return primitiveRegistry;
  }

  @Override
  public PrimitiveTypeRegistry getPrimitiveTypeRegistry() {
    return primitiveTypeRegistry;
  }

  @Override
  public PrimitiveProtocolTypeRegistry getProtocolTypeRegistry() {
    return protocolTypeRegistry;
  }

  @Override
  public PartitionGroupTypeRegistry getPartitionGroupTypeRegistry() {
    return partitionGroupTypeRegistry;
  }

  @Override
  public SessionIdService getSessionIdService() {
    return sessionIdService;
  }

  @Override
  public ThreadContextFactory getThreadFactory() {
    return threadService.getFactory();
  }
}
