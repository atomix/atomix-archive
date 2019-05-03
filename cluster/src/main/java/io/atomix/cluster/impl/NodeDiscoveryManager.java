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
package io.atomix.cluster.impl;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.NodeDiscoveryEvent;
import io.atomix.cluster.discovery.NodeDiscoveryEventListener;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.event.AbstractListenerManager;

/**
 * Default node discovery service.
 */
@Component
public class NodeDiscoveryManager
    extends AbstractListenerManager<NodeDiscoveryEvent, NodeDiscoveryEventListener>
    implements NodeDiscoveryService, Managed {

  @Dependency
  private MemberService memberService;

  @Dependency
  private BootstrapService bootstrapService;

  @Dependency
  private NodeDiscoveryProvider discoveryProvider;

  private final NodeDiscoveryEventListener discoveryEventListener = this::post;

  public NodeDiscoveryManager() {
  }

  @VisibleForTesting
  public NodeDiscoveryManager(
      MemberService memberService,
      BootstrapService bootstrapService,
      NodeDiscoveryProvider discoveryProvider) {
    this.memberService = memberService;
    this.bootstrapService = bootstrapService;
    this.discoveryProvider = discoveryProvider;
    start();
  }

  @Override
  public Set<Node> getNodes() {
    return discoveryProvider.getNodes();
  }

  @Override
  public CompletableFuture<Void> start() {
    discoveryProvider.addListener(discoveryEventListener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    discoveryProvider.removeListener(discoveryEventListener);
    return CompletableFuture.completedFuture(null);
  }
}
