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
package io.atomix.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.io.Resources;
import com.google.protobuf.Message;
import io.atomix.server.management.impl.ProtocolManager;
import io.atomix.server.protocol.LogProtocol;
import io.atomix.server.protocol.Protocol;
import io.atomix.server.protocol.ServiceProtocol;
import io.atomix.server.service.counter.CounterServiceImpl;
import io.atomix.server.service.election.LeaderElectionServiceImpl;
import io.atomix.server.service.lock.LockServiceImpl;
import io.atomix.server.service.log.LogServiceImpl;
import io.atomix.server.service.map.MapServiceImpl;
import io.atomix.server.service.set.SetServiceImpl;
import io.atomix.server.service.value.ValueServiceImpl;
import io.atomix.utils.ConfigurationException;
import io.atomix.utils.Version;
import io.atomix.utils.component.ComponentManager;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary interface for managing Atomix clusters and operating on distributed primitives.
 */
public class AtomixServer {
  private static final String VERSION_RESOURCE = "VERSION";
  private static final String BUILD;
  private static final Version VERSION;

  static {
    try {
      BUILD = Resources.toString(checkNotNull(AtomixServer.class.getClassLoader().getResource(VERSION_RESOURCE),
          VERSION_RESOURCE + " resource is null"), StandardCharsets.UTF_8);
    } catch (IOException | NullPointerException e) {
      throw new ConfigurationException("Failed to load Atomix version", e);
    }
    VERSION = BUILD.trim().length() > 0 ? Version.from(BUILD.trim().split("\\s+")[0]) : null;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixServer.class);

  private final File config;
  private volatile ComponentManager<ProtocolManager> manager;
  private volatile ProtocolManager protocolManager;
  private volatile Protocol protocol;
  private final AtomicBoolean started = new AtomicBoolean();
  private Thread shutdownHook = null;

  public AtomixServer(File config) {
    this.config = config;
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
  public synchronized CompletableFuture<AtomixServer> start() {
    manager = new ComponentManager<>(ProtocolManager.class, AtomixServer.class.getClassLoader());
    return manager.start()
        .thenCompose(protocolManager -> {
          this.protocolManager = protocolManager;
          Message protocolConfig;
          try (InputStream is = new FileInputStream(config)) {
            protocolConfig = protocolManager.getProtocolType().parseConfig(is);
          } catch (IOException e) {
            return Futures.exceptionalFuture(e);
          }
          this.protocol = protocolManager.getProtocolType().newProtocol(protocolConfig, protocolManager);
          return protocol.start();
        })
        .thenRun(() -> {
          if (protocol instanceof ServiceProtocol) {
            protocolManager.getServiceRegistry().register(new CounterServiceImpl((ServiceProtocol) protocol));
            protocolManager.getServiceRegistry().register(new LeaderElectionServiceImpl((ServiceProtocol) protocol));
            protocolManager.getServiceRegistry().register(new LockServiceImpl((ServiceProtocol) protocol));
            protocolManager.getServiceRegistry().register(new MapServiceImpl((ServiceProtocol) protocol));
            protocolManager.getServiceRegistry().register(new SetServiceImpl((ServiceProtocol) protocol));
            protocolManager.getServiceRegistry().register(new ValueServiceImpl((ServiceProtocol) protocol));
          }
          if (protocol instanceof LogProtocol) {
            protocolManager.getServiceRegistry().register(new LogServiceImpl((LogProtocol) protocol));
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
