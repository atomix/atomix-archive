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
package io.atomix.cluster;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import io.atomix.cluster.impl.ClusterManager;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.ComponentManager;
import io.atomix.utils.config.ConfigMapper;
import io.atomix.utils.net.Address;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomix cluster manager.
 * <p>
 * The cluster manager is the basis for all cluster management and communication in an Atomix cluster. This class is
 * responsible for bootstrapping new clusters or joining existing ones, establishing communication between nodes,
 * and detecting failures.
 * <p>
 * The Atomix cluster can be run as a standalone instance for cluster management and communication. To build a cluster
 * instance, use {@link #builder()} to create a new builder.
 * <pre>
 *   {@code
 *   AtomixCluster cluster = AtomixCluster.builder()
 *     .withClusterName("my-cluster")
 *     .withMemberId("member-1")
 *     .withAddress("localhost:1234")
 *     .withMulticastEnabled()
 *     .build();
 *   }
 * </pre>
 * The instance can be configured with a unique identifier via {@link AtomixClusterBuilder#withMemberId(String)}. The member ID
 * can be used to lookup the member in the {@link ClusterMembershipService} or send messages to this node from other
 * member nodes. The {@link AtomixClusterBuilder#withAddress(Address) address} is the host and port to which the node will bind
 * for intra-cluster communication over TCP.
 * <p>
 * Once an instance has been configured, the {@link #start()} method must be called to bootstrap the instance. The
 * {@code start()} method returns a {@link CompletableFuture} which will be completed once all the services have been
 * bootstrapped.
 * <pre>
 *   {@code
 *   cluster.start().join();
 *   }
 * </pre>
 */
public class AtomixCluster implements ClusterService {
  private static final String[] DEFAULT_RESOURCES = new String[]{"cluster"};

  private static String[] withDefaultResources(String config) {
    return Stream.concat(Stream.of(config), Stream.of(DEFAULT_RESOURCES)).toArray(String[]::new);
  }

  /**
   * Returns a new Atomix configuration from the given resources.
   *
   * @param resources   the resources from which to return a new Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix configuration from the given resource
   */
  private static ClusterConfig config(String[] resources, ClassLoader classLoader) {
    return new ConfigMapper(classLoader).loadResources(ClusterConfig.class, resources);
  }

  /**
   * Returns a new Atomix builder.
   *
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder() {
    return builder(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(ClassLoader classLoader) {
    return builder(config(DEFAULT_RESOURCES, classLoader));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(String config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config      the Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(String config, ClassLoader classLoader) {
    return new AtomixClusterBuilder(config(withDefaultResources(config), classLoader));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(ClusterConfig config) {
    return new AtomixClusterBuilder(config);
  }

  private final ClusterConfig config;
  private final ClassLoader classLoader;
  private final Component.Scope scope;
  private volatile ComponentManager<ClusterConfig, ClusterManager> manager;
  private volatile ClusterService clusterService;

  private final AtomicBoolean started = new AtomicBoolean();

  protected AtomixCluster(String configFile) {
    this(loadConfig(
        new File(System.getProperty("atomix.root", System.getProperty("user.dir")), configFile),
        Thread.currentThread().getContextClassLoader()), Thread.currentThread().getContextClassLoader(),
        Component.Scope.RUNTIME);
  }

  protected AtomixCluster(File configFile) {
    this(loadConfig(configFile, Thread.currentThread().getContextClassLoader()),
        Thread.currentThread().getContextClassLoader(),
        Component.Scope.RUNTIME);
  }

  protected AtomixCluster(ClusterConfig config, ClassLoader classLoader, Component.Scope scope) {
    this.config = config;
    this.classLoader = classLoader;
    this.scope = scope;
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

  /**
   * Starts the cluster.
   *
   * @return a future to be completed once the cluster has been started
   */
  public synchronized CompletableFuture<Void> start() {
    manager = new ComponentManager<>(ClusterManager.class, classLoader, scope);
    return manager.start(config).thenAccept(cluster -> {
      this.clusterService = cluster;
      started.set(true);
    });
  }

  /**
   * Returns a boolean indicating whether the cluster is running.
   *
   * @return indicates whether the cluster is running
   */
  public boolean isRunning() {
    return started.get();
  }

  /**
   * Stops the cluster.
   *
   * @return a future to be completed once the cluster has been stopped
   */
  public synchronized CompletableFuture<Void> stop() {
    return manager != null
        ? manager.stop().thenRun(() -> started.set(false))
        : CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .toString();
  }

  /**
   * Loads a configuration from the given file.
   */
  private static ClusterConfig loadConfig(File config, ClassLoader classLoader) {
    return new ConfigMapper(classLoader).loadResources(ClusterConfig.class, config.getAbsolutePath());
  }
}
