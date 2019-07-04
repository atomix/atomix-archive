/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.node;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.io.Resources;
import com.google.protobuf.Message;
import io.atomix.api.controller.PartitionConfig;
import io.atomix.node.management.impl.ConfigServiceImpl;
import io.atomix.node.management.impl.ProtocolManager;
import io.atomix.node.primitive.counter.CounterServiceImpl;
import io.atomix.node.primitive.election.LeaderElectionServiceImpl;
import io.atomix.node.primitive.list.ListServiceImpl;
import io.atomix.node.primitive.lock.LockServiceImpl;
import io.atomix.node.primitive.log.LogServiceImpl;
import io.atomix.node.primitive.map.MapServiceImpl;
import io.atomix.node.primitive.set.SetServiceImpl;
import io.atomix.node.primitive.value.ValueServiceImpl;
import io.atomix.node.protocol.LogProtocol;
import io.atomix.node.protocol.Protocol;
import io.atomix.node.protocol.ServiceProtocol;
import io.atomix.node.protocol.impl.DefaultServiceClient;
import io.atomix.node.protocol.impl.DefaultSessionClient;
import io.atomix.node.service.client.ClientFactory;
import io.atomix.node.service.client.ServiceClient;
import io.atomix.node.service.client.SessionClient;
import io.atomix.node.service.protocol.ServiceId;
import io.atomix.utils.ConfigurationException;
import io.atomix.utils.Version;
import io.atomix.utils.component.ComponentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary interface for managing Atomix clusters and operating on distributed primitives.
 */
public class AtomixNode {
  private static final String VERSION_RESOURCE = "VERSION";
  private static final String BUILD;
  private static final Version VERSION;

  static {
    try {
      BUILD = Resources.toString(checkNotNull(AtomixNode.class.getClassLoader().getResource(VERSION_RESOURCE),
          VERSION_RESOURCE + " resource is null"), StandardCharsets.UTF_8);
    } catch (IOException | NullPointerException e) {
      throw new ConfigurationException("Failed to load Atomix version", e);
    }
    VERSION = BUILD.trim().length() > 0 ? Version.from(BUILD.trim().split("\\s+")[0]) : null;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixNode.class);

  private final String nodeId;
  private final PartitionConfig partitionConfig;
  private final Message protocolConfig;
  private final Protocol.Type protocolType;
  private volatile ComponentManager<ProtocolManager> manager;
  private volatile ProtocolManager protocolManager;
  private volatile Protocol protocol;
  private final AtomicBoolean started = new AtomicBoolean();
  private Thread shutdownHook = null;

  public AtomixNode(String nodeId, PartitionConfig partitionConfig, Protocol.Type protocolType, Message protocolConfig) {
    this.nodeId = nodeId;
    this.partitionConfig = partitionConfig;
    this.protocolType = protocolType;
    this.protocolConfig = protocolConfig;
  }

  /**
   * Returns the server version.
   *
   * @return the server version
   */
  public Version getVersion() {
    return VERSION;
  }

  /**
   * Starts the Atomix instance.
   * <p>
   * The returned future will be completed once this instance completes startup. Note that in order to complete startup,
   * all partitions must be able to form. For Raft partitions, that requires that a majority of the nodes in each
   * partition be started concurrently.
   *
   * @return a future to be completed once the instance has completed startup
   */
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AtomixNode> start() {
    manager = new ComponentManager<>(ProtocolManager.class, new Object[]{new ConfigServiceImpl(nodeId, partitionConfig)}, AtomixNode.class.getClassLoader());
    return manager.start()
        .thenCompose(protocolManager -> {
          this.protocolManager = protocolManager;
          this.protocol = protocolType.newProtocol(protocolConfig, protocolManager);
          return protocol.start();
        })
        .thenRun(() -> {
          if (protocol instanceof ServiceProtocol) {
            ServiceProtocol serviceProtocol = (ServiceProtocol) protocol;
            ClientFactory factory = new ClientFactory() {
              @Override
              public ServiceClient newServiceClient(ServiceId serviceId) {
                return new DefaultServiceClient(serviceId, serviceProtocol.getServiceClient());
              }

              @Override
              public SessionClient newSessionClient(ServiceId serviceId) {
                return new DefaultSessionClient(serviceId, serviceProtocol.getServiceClient());
              }
            };
            protocolManager.getServiceRegistry().register(new CounterServiceImpl(factory));
            protocolManager.getServiceRegistry().register(new LeaderElectionServiceImpl(factory));
            protocolManager.getServiceRegistry().register(new ListServiceImpl(factory));
            protocolManager.getServiceRegistry().register(new LockServiceImpl(factory));
            protocolManager.getServiceRegistry().register(new MapServiceImpl(factory));
            protocolManager.getServiceRegistry().register(new SetServiceImpl(factory));
            protocolManager.getServiceRegistry().register(new ValueServiceImpl(factory));
          }
          if (protocol instanceof LogProtocol) {
            protocolManager.getServiceRegistry().register(new LogServiceImpl(((LogProtocol) protocol).getLogClient()));
          }
          LOGGER.info(getVersion().toString());
          started.set(true);
        }).thenRun(() -> {
          if (shutdownHook == null) {
            shutdownHook = new Thread(() -> manager.stop().join());
            Runtime.getRuntime().addShutdownHook(shutdownHook);
          }
        }).thenApply(v -> this);
  }

  /**
   * Returns a boolean indicating whether the instance is running.
   *
   * @return indicates whether the instance is running
   */
  public boolean isRunning() {
    return started.get();
  }

  /**
   * Stops the instance.
   *
   * @return a future to be completed once the instance has been stopped
   */
  public synchronized CompletableFuture<Void> stop() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        shutdownHook = null;
      } catch (IllegalStateException e) {
        // JVM shutting down
      }
    }
    return manager.stop()
        .thenRun(() -> started.set(false));
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("version", getVersion())
        .toString();
  }
}
