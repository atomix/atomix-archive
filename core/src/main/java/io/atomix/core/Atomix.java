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
package io.atomix.core;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.discovery.NodeDiscoveryConfig;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.UnicastService;
import io.atomix.cluster.protocol.GroupMembershipProtocol;
import io.atomix.cluster.protocol.GroupMembershipProtocolConfig;
import io.atomix.core.impl.AtomixManager;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.utils.config.PolymorphicConfigMapper;
import io.atomix.core.utils.config.PolymorphicTypeMapper;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.utils.Version;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.ComponentManager;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.config.ConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

/**
 * Primary interface for managing Atomix clusters and operating on distributed primitives.
 * <p>
 * The {@code Atomix} class is the primary interface to all Atomix features. To construct an {@code Atomix} instance,
 * either configure the instance with a configuration file or construct a new instance from an {@link AtomixBuilder}.
 * Builders can be created via various {@link #builder()} methods:
 * <pre>
 *   {@code
 *   Atomix atomix = Atomix.builder()
 *     .withMemberId("member-1")
 *     .withHost("192.168.10.2")
 *     .build();
 *   }
 * </pre>
 * Once an {@code Atomix} instance has been constructed, start the instance by calling {@link #start()}:
 * <pre>
 *   {@code
 *   atomix.start().join();
 *   }
 * </pre>
 * The returned {@link CompletableFuture} will be completed once the node has been bootstrapped and all services are
 * available.
 * <p>
 * The instance can be used to access services for managing the cluster or communicating with other nodes. Additionally,
 * it provides various methods for creating and operating on distributed primitives. Generally, the primitive methods
 * are separated into two types. Primitive getters return multiton instances of a primitive. Primitives created via
 * getters must be pre-configured in the Atomix instance configuration. Alternatively, primitive builders can be used to
 * create and configure primitives in code:
 * <pre>
 *   {@code
 *   DistributedMap<String, String> map = atomix.mapBuilder("my-map")
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 * Custom primitives can be constructed by providing a custom {@link PrimitiveType} and using the {@link
 * #primitiveBuilder(String, PrimitiveType)} method:
 * <pre>
 *   {@code
 *   MyPrimitive myPrimitive = atomix.primitiveBuilder("my-primitive, MyPrimitiveType.instance())
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 */
public class Atomix implements AtomixService {
  private static final String[] RESOURCES = System.getProperty("atomix.config.resources", "atomix").split(",");

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from {@code atomix.conf}, {@code atomix.json}, or {@code atomix.properties} if
   * located on the classpath.
   *
   * @return a new Atomix configuration
   */
  public static AtomixConfig config() {
    return config(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from {@code atomix.conf}, {@code atomix.json}, or {@code atomix.properties} if
   * located on the classpath.
   *
   * @param classLoader the class loader
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(ClassLoader classLoader) {
    return config(classLoader, null, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from {@code atomix.conf}, {@code atomix.json}, or {@code atomix.properties} if
   * located on the classpath.
   *
   * @param registry the Atomix registry
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(AtomixRegistry registry) {
    return config(Thread.currentThread().getContextClassLoader(), null, registry);
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code
   * atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param files the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(String... files) {
    return config(Thread.currentThread().getContextClassLoader(), Stream.of(files).map(File::new).collect(Collectors.toList()));
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code
   * atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param classLoader the class loader
   * @param files       the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(ClassLoader classLoader, String... files) {
    return config(classLoader, Stream.of(files).map(File::new).collect(Collectors.toList()), AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code
   * atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param registry the Atomix registry
   * @param files    the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(AtomixRegistry registry, String... files) {
    return config(Thread.currentThread().getContextClassLoader(), Stream.of(files).map(File::new).collect(Collectors.toList()), registry);
  }

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code
   * atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param configFiles the Atomix configuration files
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(File... configFiles) {
    return config(Thread.currentThread().getContextClassLoader(), Arrays.asList(configFiles), AtomixRegistry.registry());
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code
   * atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param files the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(List<File> files) {
    return config(Thread.currentThread().getContextClassLoader(), files);
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code
   * atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param classLoader the class loader
   * @param files       the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(ClassLoader classLoader, List<File> files) {
    return config(classLoader, files, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code
   * atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param registry the Atomix registry
   * @param files    the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(AtomixRegistry registry, List<File> files) {
    return config(Thread.currentThread().getContextClassLoader(), files, registry);
  }

  /**
   * Returns a new Atomix configuration from the given resources.
   *
   * @param classLoader the class loader
   * @param files       the files to load
   * @param registry    the Atomix registry from which to map types
   * @return a new Atomix configuration from the given resource
   */
  private static AtomixConfig config(ClassLoader classLoader, List<File> files, AtomixRegistry registry) {
    ConfigMapper mapper = new PolymorphicConfigMapper(
        classLoader,
        registry,
        new PolymorphicTypeMapper("type", PartitionGroupConfig.class, PartitionGroup.Type.class),
        new PolymorphicTypeMapper("type", PrimitiveConfig.class, PrimitiveType.class),
        new PolymorphicTypeMapper(null, PrimitiveConfig.class, PrimitiveType.class),
        new PolymorphicTypeMapper("type", PrimitiveProtocolConfig.class, PrimitiveProtocol.Type.class),
        new PolymorphicTypeMapper("type", NodeDiscoveryConfig.class, NodeDiscoveryProvider.Type.class),
        new PolymorphicTypeMapper("type", GroupMembershipProtocolConfig.class, GroupMembershipProtocol.Type.class));
    return mapper.loadFiles(AtomixConfig.class, files, Lists.newArrayList(RESOURCES));
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in {@code atomix.conf}, {@code atomix.json}, or {@code
   * atomix.properties} if located on the classpath.
   *
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder() {
    return builder(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in {@code atomix.conf}, {@code atomix.json}, or {@code
   * atomix.properties} if located on the classpath.
   *
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(ClassLoader classLoader) {
    return new AtomixBuilder(config(classLoader, null, AtomixRegistry.registry(classLoader)), classLoader);
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in the given file and will fall back to {@code atomix.conf},
   * {@code atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param registry the AtomixRegistry
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(AtomixRegistry registry) {
    return new AtomixBuilder(config(
        Thread.currentThread().getContextClassLoader(),
        null,
        registry),
        Thread.currentThread().getContextClassLoader());
  }


  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in the given file and will fall back to {@code atomix.conf},
   * {@code atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(String config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in the given file and will fall back to {@code atomix.conf},
   * {@code atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param configFile  the Atomix configuration file
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(String configFile, ClassLoader classLoader) {
    return new AtomixBuilder(config(
        classLoader,
        Collections.singletonList(new File(configFile)),
        AtomixRegistry.registry(classLoader)),
        classLoader);
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in the given file and will fall back to {@code atomix.conf},
   * {@code atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param configFile the Atomix configuration file
   * @param registry   the Atomix registry
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(String configFile, AtomixRegistry registry) {
    return new AtomixBuilder(config(
        Thread.currentThread().getContextClassLoader(),
        Collections.singletonList(new File(configFile)),
        registry),
        Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The returned builder will be initialized with the provided configuration.
   *
   * @param config the Atomix configuration
   * @return the Atomix builder
   */
  public static AtomixBuilder builder(AtomixConfig config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The returned builder will be initialized with the provided configuration.
   *
   * @param config      the Atomix configuration
   * @param classLoader the class loader with which to load the Atomix registry
   * @return the Atomix builder
   */
  public static AtomixBuilder builder(AtomixConfig config, ClassLoader classLoader) {
    return new AtomixBuilder(config, classLoader);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Atomix.class);

  private final AtomixConfig config;
  private final ClassLoader classLoader;
  private final Component.Scope scope;
  private volatile ComponentManager<AtomixConfig, AtomixManager> manager;
  private volatile AtomixService atomixService;
  private final boolean enableShutdownHook;
  private final AtomicBoolean started = new AtomicBoolean();
  private Thread shutdownHook = null;

  public Atomix(String... configFiles) {
    this(Thread.currentThread().getContextClassLoader(), configFiles);
  }

  public Atomix(ClassLoader classLoader, String... configFiles) {
    this(classLoader, Stream.of(configFiles).map(File::new).collect(Collectors.toList()));
  }

  public Atomix(File... configFiles) {
    this(Thread.currentThread().getContextClassLoader(), configFiles);
  }

  public Atomix(ClassLoader classLoader, File... configFiles) {
    this(classLoader, Arrays.asList(configFiles));
  }

  public Atomix(ClassLoader classLoader, List<File> configFiles) {
    this(config(classLoader, configFiles, AtomixRegistry.registry(classLoader)));
  }

  protected Atomix(AtomixConfig config) {
    this(config, Thread.currentThread().getContextClassLoader(), Component.Scope.RUNTIME);
  }

  protected Atomix(AtomixConfig config, Component.Scope scope) {
    this(config, Thread.currentThread().getContextClassLoader(), scope);
  }

  protected Atomix(AtomixConfig config, ClassLoader classLoader, Component.Scope scope) {
    this.config = config;
    this.classLoader = classLoader;
    this.scope = scope;
    this.enableShutdownHook = config.isEnableShutdownHook();
  }

  @Override
  public Version getVersion() {
    return atomixService.getVersion();
  }

  @Override
  public UnicastService getUnicastService() {
    return atomixService.getUnicastService();
  }

  @Override
  public BroadcastService getBroadcastService() {
    return atomixService.getBroadcastService();
  }

  @Override
  public MessagingService getMessagingService() {
    return atomixService.getMessagingService();
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return atomixService.getMembershipService();
  }

  @Override
  public ClusterCommunicationService getCommunicationService() {
    return atomixService.getCommunicationService();
  }

  @Override
  public ClusterStreamingService getStreamingService() {
    return atomixService.getStreamingService();
  }

  @Override
  public ClusterEventService getEventService() {
    return atomixService.getEventService();
  }

  @Override
  public PrimitiveTypeRegistry getPrimitiveTypes() {
    return atomixService.getPrimitiveTypes();
  }

  @Override
  public PrimitiveProtocolTypeRegistry getProtocolTypes() {
    return atomixService.getProtocolTypes();
  }

  @Override
  public PartitionGroupTypeRegistry getPartitionGroupTypes() {
    return atomixService.getPartitionGroupTypes();
  }

  @Override
  public ThreadContextFactory getThreadFactory() {
    return atomixService.getThreadFactory();
  }

  @Override
  public ConfigService getConfigService() {
    return atomixService.getConfigService();
  }

  @Override
  public PartitionService getPartitionService() {
    return atomixService.getPartitionService();
  }

  @Override
  public TransactionService getTransactionService() {
    return atomixService.getTransactionService();
  }

  @Override
  public SessionIdService getSessionIdService() {
    return atomixService.getSessionIdService();
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    checkRunning();
    return atomixService.transactionBuilder(name);
  }

  @Override
  public <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends SyncPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType) {
    checkRunning();
    return atomixService.primitiveBuilder(name, primitiveType);
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    checkRunning();
    return atomixService.getPrimitiveType(typeName);
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    checkRunning();
    return atomixService.getPrimitives();
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    checkRunning();
    return atomixService.getPrimitives(primitiveType);
  }

  /**
   * Checks that the instance is running.
   */
  private void checkRunning() {
    checkState(isRunning(), "Atomix instance is not running");
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
  public synchronized CompletableFuture<Atomix> start() {
    manager = new ComponentManager<>(AtomixManager.class, classLoader, scope);
    return manager.start(config).thenAccept(atomixManager -> {
      this.atomixService = atomixManager;
      LOGGER.info(atomixService.getVersion().toString());
      started.set(true);
    }).thenRun(() -> {
      if (enableShutdownHook) {
        if (shutdownHook == null) {
          shutdownHook = new Thread(() -> manager.stop().join());
          Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
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
        .add("partitions", getPartitionService())
        .toString();
  }
}
