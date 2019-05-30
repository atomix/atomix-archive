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
package io.atomix.server;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import io.atomix.server.management.impl.ServerManager;
import io.atomix.utils.Version;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.ComponentManager;
import io.atomix.utils.config.ConfigMapper;
import io.atomix.utils.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary interface for managing Atomix clusters and operating on distributed primitives.
 */
public class AtomixServer {
  private static final String[] RESOURCES = System.getProperty("atomix.config.resources", "atomix.yaml").split(",");

  private static final String VERSION_RESOURCE = "VERSION";
  private static final String BUILD;
  private static final Version VERSION;

  static {
    try {
      BUILD = Resources.toString(checkNotNull(ServerManager.class.getClassLoader().getResource(VERSION_RESOURCE),
          VERSION_RESOURCE + " resource is null"), StandardCharsets.UTF_8);
    } catch (IOException | NullPointerException e) {
      throw new ConfigurationException("Failed to load Atomix version", e);
    }
    VERSION = BUILD.trim().length() > 0 ? Version.from(BUILD.trim().split("\\s+")[0]) : null;
  }

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from {@code atomix.conf}, {@code atomix.json}, or {@code atomix.properties} if
   * located on the classpath.
   *
   * @return a new Atomix configuration
   */
  public static ServerConfig config() {
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
  public static ServerConfig config(ClassLoader classLoader) {
    return config(classLoader, Collections.emptyList());
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
  public static ServerConfig config(String... files) {
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
  public static ServerConfig config(ClassLoader classLoader, String... files) {
    return config(classLoader, Stream.of(files).map(File::new).collect(Collectors.toList()));
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
  public static ServerConfig config(File... configFiles) {
    return config(Thread.currentThread().getContextClassLoader(), Arrays.asList(configFiles));
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
  public static ServerConfig config(List<File> files) {
    return config(Thread.currentThread().getContextClassLoader(), files);
  }

  /**
   * Returns a new Atomix configuration from the given resources.
   *
   * @param classLoader the class loader
   * @param files       the files to load
   * @return a new Atomix configuration from the given resource
   */
  private static ServerConfig config(ClassLoader classLoader, List<File> files) {
    ConfigMapper mapper = new ConfigMapper(classLoader);
    return mapper.loadFiles(ServerConfig.getDescriptor(), files, Lists.newArrayList(RESOURCES));
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixServer.class);

  private final ServerConfig config;
  private final ClassLoader classLoader;
  private final Component.Scope scope;
  private volatile ComponentManager<ServerConfig, ServerManager> manager;
  private volatile ServerManager serverManager;
  private final AtomicBoolean started = new AtomicBoolean();
  private Thread shutdownHook = null;

  public AtomixServer(String... configFiles) {
    this(Thread.currentThread().getContextClassLoader(), configFiles);
  }

  public AtomixServer(ClassLoader classLoader, String... configFiles) {
    this(classLoader, Stream.of(configFiles).map(File::new).collect(Collectors.toList()));
  }

  public AtomixServer(File... configFiles) {
    this(Thread.currentThread().getContextClassLoader(), configFiles);
  }

  public AtomixServer(ClassLoader classLoader, File... configFiles) {
    this(classLoader, Arrays.asList(configFiles));
  }

  public AtomixServer(ClassLoader classLoader, List<File> configFiles) {
    this(config(classLoader, configFiles));
  }

  public AtomixServer(ServerConfig config) {
    this(config, Thread.currentThread().getContextClassLoader(), Component.Scope.RUNTIME);
  }

  protected AtomixServer(ServerConfig config, Component.Scope scope) {
    this(config, Thread.currentThread().getContextClassLoader(), scope);
  }

  protected AtomixServer(ServerConfig config, ClassLoader classLoader, Component.Scope scope) {
    this.config = config;
    this.classLoader = classLoader;
    this.scope = scope;
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
  public synchronized CompletableFuture<AtomixServer> start() {
    manager = new ComponentManager<>(ServerManager.class, classLoader, scope);
    return manager.start(config).thenAccept(atomixManager -> {
      this.serverManager = atomixManager;
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
