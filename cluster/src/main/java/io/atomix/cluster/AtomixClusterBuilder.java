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

import io.atomix.utils.component.Component;

/**
 * Builder for an {@link AtomixCluster} instance.
 * <p>
 * This builder is used to configure an {@link AtomixCluster} instance programmatically. To create a new builder, use
 * one of the {@link AtomixCluster#builder()} static methods.
 * <pre>
 *   {@code
 *   AtomixClusterBuilder builder = AtomixCluster.builder();
 *   }
 * </pre>
 * The instance is configured by calling the {@code with*} methods on this builder. Once the instance has been
 * configured, call {@link #build()} to build the instance:
 * <pre>
 *   {@code
 *   AtomixCluster cluster = AtomixCluster.builder()
 *     .withMemberId("member-1")
 *     .withAddress("localhost", 5000)
 *     .build();
 *   }
 * </pre>
 * Backing the builder is an {@link ClusterConfig} which is loaded when the builder is initially constructed. To load
 * a configuration from a file, use {@link AtomixCluster#builder(String)}.
 */
public class AtomixClusterBuilder extends AbstractClusterBuilder<AtomixCluster> {
  public AtomixClusterBuilder() {
    super(new ClusterConfig());
  }

  public AtomixClusterBuilder(ClusterConfig config) {
    super(config);
  }

  @Override
  public AtomixCluster build() {
    return new AtomixCluster(config, AtomixClusterBuilder.class.getClassLoader(), Component.Scope.RUNTIME);
  }
}
