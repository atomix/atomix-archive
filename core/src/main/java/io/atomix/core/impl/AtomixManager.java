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

import java.util.Collection;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.VersionService;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.UnicastService;
import io.atomix.core.AtomixConfig;
import io.atomix.core.AtomixService;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionConfig;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.transaction.impl.DefaultTransactionBuilder;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.PrimitiveSessionIdManager;
import io.atomix.utils.Version;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadService;

/**
 * Default primitives service.
 */
@Component(AtomixConfig.class)
public class AtomixManager implements AtomixService, Managed<AtomixConfig> {

  @Dependency
  private VersionService versionService;

  @Dependency
  private ConfigService configService;

  @Dependency
  private PartitionService partitions;

  @Dependency
  private TransactionService transactionService;

  @Dependency
  private PrimitiveSessionIdManager sessionIdService;

  @Dependency
  private PrimitiveTypeRegistry primitiveTypes;

  @Dependency
  private PrimitiveProtocolTypeRegistry protocolTypes;

  @Dependency
  private PartitionGroupTypeRegistry partitionGroupTypes;

  @Dependency
  private ThreadService threadService;

  @Dependency
  private PrimitiveManagementService managementService;

  @Dependency
  private ClusterService clusterService;

  /**
   * Returns the primitive management service.
   *
   * @return the primitive management service
   */
  public PrimitiveManagementService getManagementService() {
    return managementService;
  }

  /**
   * Returns the primitive transaction service.
   *
   * @return the primitive transaction service
   */
  public TransactionService transactionService() {
    return transactionService;
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    return new DefaultTransactionBuilder(name, new TransactionConfig(), managementService, transactionService);
  }

  @Override
  public Version getVersion() {
    return versionService.version();
  }

  @Override
  public ThreadContextFactory getThreadFactory() {
    return threadService.getFactory();
  }

  @Override
  public UnicastService getUnicastService() {
    return clusterService.getUnicastService();
  }

  @Override
  public BroadcastService getBroadcastService() {
    return clusterService.getBroadcastService();
  }

  @Override
  public MessagingService getMessagingService() {
    return clusterService.getMessagingService();
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return clusterService.getMembershipService();
  }

  @Override
  public ClusterCommunicationService getCommunicationService() {
    return clusterService.getCommunicationService();
  }

  @Override
  public ClusterStreamingService getStreamingService() {
    return clusterService.getStreamingService();
  }

  @Override
  public ClusterEventService getEventService() {
    return clusterService.getEventService();
  }

  @Override
  public ConfigService getConfigService() {
    return configService;
  }

  @Override
  public PartitionService getPartitionService() {
    return partitions;
  }

  @Override
  public TransactionService getTransactionService() {
    return transactionService;
  }

  @Override
  public SessionIdService getSessionIdService() {
    return sessionIdService;
  }

  @Override
  public PrimitiveTypeRegistry getPrimitiveTypes() {
    return primitiveTypes;
  }

  @Override
  public PrimitiveProtocolTypeRegistry getProtocolTypes() {
    return protocolTypes;
  }

  @Override
  public PartitionGroupTypeRegistry getPartitionGroupTypes() {
    return partitionGroupTypes;
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    return primitiveTypes.getPrimitiveType(typeName);
  }

  @Override
  public <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends SyncPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, C, P> primitiveType) {
    return primitiveType.newBuilder(name, configService.getConfig(name, primitiveType), managementService);
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    return managementService.getPrimitiveRegistry().getPrimitives();
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return managementService.getPrimitiveRegistry().getPrimitives(primitiveType);
  }
}
