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
import io.atomix.utils.component.Component;
import io.atomix.utils.component.ComponentManager;
import io.atomix.utils.config.ConfigMapper;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomix cluster manager.
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
    return new ConfigMapper(classLoader).loadResources(ClusterConfig.getDescriptor(), resources);
  }

  private final ClusterConfig config;
  private final ClassLoader classLoader;
  private final Component.Scope scope;
  private volatile ComponentManager<ClusterConfig, ClusterManager> manager;
  private volatile ClusterService clusterService;

  private final AtomicBoolean started = new AtomicBoolean();

  public AtomixCluster(String configFile) {
    this(loadConfig(
        new File(System.getProperty("atomix.root", System.getProperty("user.dir")), configFile),
        Thread.currentThread().getContextClassLoader()), Thread.currentThread().getContextClassLoader(),
        Component.Scope.RUNTIME);
  }

  public AtomixCluster(File configFile) {
    this(loadConfig(configFile, Thread.currentThread().getContextClassLoader()),
        Thread.currentThread().getContextClassLoader(),
        Component.Scope.RUNTIME);
  }

  public AtomixCluster(ClusterConfig config) {
    this(config, Thread.currentThread().getContextClassLoader(), Component.Scope.RUNTIME);
  }

  protected AtomixCluster(ClusterConfig config, ClassLoader classLoader, Component.Scope scope) {
    this.config = config;
    this.classLoader = classLoader;
    this.scope = scope;
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return clusterService.getMembershipService();
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
    return new ConfigMapper(classLoader).loadResources(ClusterConfig.getDescriptor(), config.getAbsolutePath());
  }
}
