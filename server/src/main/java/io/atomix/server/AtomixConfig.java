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
package io.atomix.server;

import io.atomix.cluster.ClusterConfig;
import io.atomix.primitive.partition.PartitionGroupsConfig;
import io.atomix.utils.config.Config;

/**
 * Atomix configuration.
 */
public class AtomixConfig extends PartitionGroupsConfig implements Config {
  private static final String MANAGEMENT_GROUP_NAME = "system";

  private ClusterConfig cluster = new ClusterConfig();
  private boolean enableShutdownHook;
  private boolean typeRegistrationRequired = false;
  private boolean compatibleSerialization = false;

  /**
   * Returns the cluster configuration.
   *
   * @return the cluster configuration
   */
  public ClusterConfig getClusterConfig() {
    return cluster;
  }

  /**
   * Sets the cluster configuration.
   *
   * @param cluster the cluster configuration
   * @return the Atomix configuration
   */
  public AtomixConfig setClusterConfig(ClusterConfig cluster) {
    this.cluster = cluster;
    return this;
  }

  /**
   * Returns whether to enable the shutdown hook.
   *
   * @return whether to enable the shutdown hook
   */
  public boolean isEnableShutdownHook() {
    return enableShutdownHook;
  }

  /**
   * Sets whether to enable the shutdown hook.
   *
   * @param enableShutdownHook whether to enable the shutdown hook
   * @return the Atomix configuration
   */
  public AtomixConfig setEnableShutdownHook(boolean enableShutdownHook) {
    this.enableShutdownHook = enableShutdownHook;
    return this;
  }

  /**
   * Returns whether serializable type registration is required for user types.
   *
   * @return whether serializable type registration is required for user types
   */
  public boolean isTypeRegistrationRequired() {
    return typeRegistrationRequired;
  }

  /**
   * Sets whether serializable type registration is required for user types.
   *
   * @param typeRegistrationRequired whether serializable type registration is required for user types
   * @return the Atomix configuration
   */
  public AtomixConfig setTypeRegistrationRequired(boolean typeRegistrationRequired) {
    this.typeRegistrationRequired = typeRegistrationRequired;
    return this;
  }

  /**
   * Returns whether compatible serialization is enabled for user types.
   *
   * @return whether compatible serialization is enabled for user types
   */
  public boolean isCompatibleSerialization() {
    return compatibleSerialization;
  }

  /**
   * Sets whether compatible serialization is enabled for user types.
   *
   * @param compatibleSerialization whether compatible serialization is enabled for user types
   * @return the Atomix configuration
   */
  public AtomixConfig setCompatibleSerialization(boolean compatibleSerialization) {
    this.compatibleSerialization = compatibleSerialization;
    return this;
  }
}
