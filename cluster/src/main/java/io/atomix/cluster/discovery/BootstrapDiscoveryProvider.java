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
package io.atomix.cluster.discovery;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableSet;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import io.atomix.utils.event.AbstractListenable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster membership provider that bootstraps membership from a pre-defined set of peers.
 * <p>
 * The bootstrap member provider takes a set of peer nodes and uses them to join the cluster. Using gRPC, each node
 * sends a heartbeat to its configured bootstrap peers. Peers respond to each heartbeat message with a list of all
 * known peers, thus propagating membership information using a gossip style protocol.
 * <p>
 * A phi accrual failure detector is used to detect failures and remove peers from the configuration. In order to avoid
 * flapping of membership following a {@link io.atomix.cluster.MemberEvent.Type#ADDED} event, the implementation attempts
 * to heartbeat all newly discovered peers before triggering a {@link io.atomix.cluster.MemberEvent.Type#REMOVED} event.
 */
@Component(BootstrapDiscoveryConfig.class)
public class BootstrapDiscoveryProvider
    extends AbstractListenable<DiscoveryEvent>
    implements NodeDiscoveryProvider<BootstrapDiscoveryConfig>, Managed<BootstrapDiscoveryConfig> {

  public static final Type TYPE = new Type();

  /**
   * Bootstrap member location provider type.
   */
  public static class Type implements NodeDiscoveryProvider.Type<BootstrapDiscoveryConfig> {
    private static final String NAME = "bootstrap";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public BootstrapDiscoveryConfig newConfig() {
      return BootstrapDiscoveryConfig.newBuilder().build();
    }

    @Override
    public NodeDiscoveryProvider newProvider(BootstrapDiscoveryConfig config) {
      return new BootstrapDiscoveryProvider(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapDiscoveryProvider.class);

  private ImmutableSet<Node> bootstrapNodes;
  private BootstrapDiscoveryConfig config;

  private BootstrapDiscoveryProvider() {
  }

  public BootstrapDiscoveryProvider(Node... bootstrapNodes) {
    this(Arrays.asList(bootstrapNodes));
  }

  public BootstrapDiscoveryProvider(Collection<Node> bootstrapNodes) {
    this(BootstrapDiscoveryConfig.newBuilder()
        .addAllNodes(bootstrapNodes)
        .build());
  }

  BootstrapDiscoveryProvider(BootstrapDiscoveryConfig config) {
    this.config = checkNotNull(config);
  }

  @Override
  public BootstrapDiscoveryConfig config() {
    return config;
  }

  @Override
  public Set<Node> getNodes() {
    return bootstrapNodes;
  }

  @Override
  public CompletableFuture<Void> start(BootstrapDiscoveryConfig config) {
    LOGGER.info("Joined");
    this.config = config;
    this.bootstrapNodes = ImmutableSet.copyOf(config.getNodesList());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    LOGGER.info("Left");
    return CompletableFuture.completedFuture(null);
  }
}
